package com.onework.core.job.parser;

import com.google.common.collect.Sets;
import com.onework.core.entity.BatchJob;
import com.onework.core.entity.SqlStatement;
import com.onework.core.enums.EngineKind;
import com.onework.core.enums.StatementKind;
import com.onework.core.job.parser.statement.DependentSqlParser;
import com.onework.core.job.parser.statement.JobEntryParser;
import com.onework.core.job.parser.statement.SqlStatementParser;
import com.onework.core.job.parser.statement.StatementParser;
import com.onework.core.pattern.PatternReplacerFactory;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

@Component
public class BatchJobParser extends BaseJobParser<BatchJob> {
    private Set<String> engineKinds = Sets.newHashSet(
            Stream.of(EngineKind.values()).map(Enum::name).collect(Collectors.toList()));
    private PatternReplacerFactory patternReplacerFactory;

    @Autowired
    public BatchJobParser(PatternReplacerFactory patternReplacerFactory) {
        this.patternReplacerFactory = patternReplacerFactory;
    }

    @Override
    protected void bindParser(Map<StatementKind, StatementParser> statementParsers) {
        statementParsers.put(StatementKind.JOB_ENTRY, new JobEntryParser());
        statementParsers.put(StatementKind.DEPENDENT_SQL, new DependentSqlParser());
        statementParsers.put(StatementKind.SQL_STATEMENT, new SqlStatementParser());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected BatchJob onCreateJob(List<Map<String, Object>> statementsData) {
        BatchJob batchJob = new BatchJob();
        List<String> dependentJobNames = new ArrayList<>();
        List<SqlStatement> jobSqlStatements = new ArrayList<>();
        for (Map<String, Object> statementData : statementsData) {
            StatementKind statementKind = getStatementKind(statementData);
            switch (statementKind) {
                case JOB_ENTRY:
                    parseJobEntry(statementData, batchJob);
                    Map<String, String> jobParams = (Map<String, String>) statementData.get("jobParams");
                    String engine = jobParams.get("engine");
                    checkArgument(StringUtils.isNotEmpty(engine) && engineKinds.contains(engine.toUpperCase()));
                    EngineKind engineKind = EngineKind.valueOf(engine.toUpperCase());
                    batchJob.setEngineKind(engineKind);
                    String cronTime = jobParams.get("cronTime");
                    checkArgument(StringUtils.isNotEmpty(cronTime) && CronExpression.isValidExpression(cronTime));
                    batchJob.setCronTime(cronTime);
                    break;
                case DEPENDENT_SQL:
                    parseDependentJob(statementData, dependentJobNames);
                    break;
                case SQL_STATEMENT:
                    checkArgument(statementData.containsKey("sqlStatements"));
                    List<SqlStatement> sqlStatements = ((List<String>) statementData.get("sqlStatements")).stream()
                            .map(s -> new SqlStatement(batchJob.getJobName(), s)).collect(Collectors.toList());
                    patternReplacerFactory.patternReplace(sqlStatements);
                    jobSqlStatements.addAll(sqlStatements);
                    break;
                default:
                    break;
            }
        }
        batchJob.setDependentJobNames(dependentJobNames);
        batchJob.setSqlStatements(jobSqlStatements);
        return batchJob;
    }
}
