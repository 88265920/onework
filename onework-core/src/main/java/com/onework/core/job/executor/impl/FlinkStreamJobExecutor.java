package com.onework.core.job.executor.impl;

import com.onework.core.client.HdfsCheckpointManager;
import com.onework.core.client.YarnClusterClient;
import com.onework.core.conf.OneWorkConf;
import com.onework.core.entity.SqlStatement;
import com.onework.core.entity.StreamJob;
import com.onework.core.enums.JobStatus;
import com.onework.core.job.executor.StreamJobExecutor;
import com.onework.core.service.StreamJobService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@Slf4j
@Component
public class FlinkStreamJobExecutor implements StreamJobExecutor {
    private OneWorkConf oneWorkConf;
    private YarnClusterClient yarnClusterClient;
    private HdfsCheckpointManager hdfsCheckPointManager;
    private StreamJobService streamJobService;

    @Autowired
    public FlinkStreamJobExecutor(OneWorkConf oneWorkConf, YarnClusterClient yarnClusterClient,
                                  HdfsCheckpointManager hdfsCheckPointManager, StreamJobService streamJobService) {
        this.oneWorkConf = oneWorkConf;
        this.yarnClusterClient = yarnClusterClient;
        this.hdfsCheckPointManager = hdfsCheckPointManager;
        this.streamJobService = streamJobService;
    }

    @Override
    public void executeJob(@NonNull StreamJob job) {
        if (StringUtils.isNotEmpty(job.getCheckpointJobId())) {
            String savepoint = hdfsCheckPointManager.getLatestCheckPoint(job.getCheckpointJobId());
            checkState(StringUtils.isNotEmpty(savepoint));
            StreamTableEnvironment tableEnv = createExecutor(job.getJobEntry().getJobParams(), savepoint);
            execute(job, tableEnv);
        }
    }

    @Override
    public void executeJobWithSavepoint(@NonNull StreamJob job) {
        StreamTableEnvironment tableEnv = createExecutor(job.getJobEntry().getJobParams(), job.getSavepoint());
        execute(job, tableEnv);
    }

    @Override
    public void executeJobWithoutState(@NonNull StreamJob job) {
        StreamTableEnvironment tableEnv = createExecutor(job.getJobEntry().getJobParams(), null);
        execute(job, tableEnv);
    }

    private StreamTableEnvironment createExecutor(Map<String, String> jobParams, String savepoint) {
        String jarsParam = jobParams.get("jars");
        StreamExecutionEnvironment env;
        Configuration clientConfig = new Configuration();
        if (StringUtils.isNotEmpty(savepoint)) {
            clientConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, savepoint);
            clientConfig.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false);
        }

        if (StringUtils.isNotEmpty(jarsParam)) {
            String[] jars = Stream.of(jarsParam.replace("'", "").split(","))
                    .map(String::trim).toArray(String[]::new);
            env = StreamExecutionEnvironment.createRemoteEnvironment(yarnClusterClient.getSessionHost(),
                    yarnClusterClient.getSessionPort(), clientConfig, jars);
        } else {
            env = StreamExecutionEnvironment.createRemoteEnvironment(yarnClusterClient.getSessionHost(),
                    yarnClusterClient.getSessionPort(), clientConfig);
        }

        env.setRestartStrategy(RestartStrategies.noRestart());

        String parallelism = jobParams.get("parallelism");
        checkArgument(StringUtils.isNotEmpty(parallelism));
        env.setParallelism(Integer.parseInt(parallelism));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(20000);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setPreferCheckpointForRecovery(true);
        checkpointConfig.setMinPauseBetweenCheckpoints(120000);

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.getConfiguration().setString("table.exec.sink.not-null-enforcer", "drop");

        String stateRetentionHour = jobParams.get("stateRetentionHour");
        if (StringUtils.isNotEmpty(stateRetentionHour)) {
            checkArgument(Integer.parseInt(stateRetentionHour) > 0);
            tableConfig.setIdleStateRetentionTime(Time.hours(Integer.parseInt(stateRetentionHour)),
                    Time.hours(Integer.parseInt(stateRetentionHour + 1)));
        }

        try {
            env.setStateBackend((StateBackend) new RocksDBStateBackend(oneWorkConf.getFlink().getCheckpoint(),
                    true));
        } catch (IOException e) {
            log.error("", e);
        }

        Map<String, String> globalJobParams = new HashMap<>();
        for (Map.Entry<String, String> entry : jobParams.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("gp.")) {
                key = key.substring(key.indexOf('.') + 1);
                globalJobParams.put(key, entry.getValue());
            }
        }

        ExecutionConfig config = env.getConfig();
        config.enableForceKryo();
        config.enableObjectReuse();
        config.setGlobalJobParameters(ParameterTool.fromMap(globalJobParams));
        return tableEnv;
    }

    private void execute(StreamJob job, StreamTableEnvironment tableEnv) {
        Map<String, String> jobParams = job.getJobEntry().getJobParams();
        String isDependentJob = jobParams.get("dependent_job");
        if (isDependentJob != null && isDependentJob.equalsIgnoreCase("true")) return;
        List<String> dependentJobNames = job.getDependentJobNames();
        checkState(!dependentJobNames.isEmpty());
        StatementSet statementSet = tableEnv.createStatementSet();
        for (String dependentJobName : dependentJobNames) {
            StreamJob dependentJob = streamJobService.findByName(dependentJobName);
            executeSql(dependentJob.getSqlStatements(), tableEnv, statementSet);
        }
        executeSql(job.getSqlStatements(), tableEnv, statementSet);
        TableResult tableResult = statementSet.execute();
        Optional<JobClient> jobClient = tableResult.getJobClient();
        String jobId = null;
        if (jobClient.isPresent()) jobId = jobClient.get().getJobID().toString();
        if (!streamJobService.existsByJobName(job.getJobName())) {
            job.setJobId(jobId);
            job.setJobStatus(JobStatus.RUNNING);
            streamJobService.save(job);
        } else {
            streamJobService.setStatusAndIdByName(JobStatus.RUNNING, jobId, job.getJobName());
        }
    }

    private void executeSql(@NonNull List<SqlStatement> sqlStatements, StreamTableEnvironment tableEnv,
                            StatementSet statementSet) {
        for (SqlStatement sqlStatement : sqlStatements) {
            String sql = sqlStatement.getSqlContent();
            log.info(sql);
            if (sql.startsWith("INSERT") || sql.startsWith("insert")) {
                statementSet.addInsertSql(sql);
            } else {
                tableEnv.executeSql(sql);
            }
        }
    }
}
