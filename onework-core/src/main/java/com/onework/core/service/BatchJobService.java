package com.onework.core.service;

import com.onework.core.entity.BatchJob;
import com.onework.core.enums.EngineKind;
import com.onework.core.enums.JobStatus;
import com.onework.core.job.executor.BatchJobExecutor;
import com.onework.core.job.executor.ExecutePositionTracker;
import com.onework.core.job.executor.impl.JDBCBatchJobExecutor;
import com.onework.core.job.parser.BatchJobParser;
import com.onework.core.repository.BatchJobRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Slf4j
@Service
public class BatchJobService {
    private BatchJobRepository batchJobRepository;
    private BatchJobParser batchJobParser;
    private Map<EngineKind, BatchJobExecutor> batchJobExecutors;
    private ExecutePositionTracker executePositionTracker = (String jobName, Date fireTime, int pos) ->
            batchJobRepository.setFireTimeAndExecutePositionByName(fireTime, pos, jobName);

    @Autowired
    public BatchJobService(BatchJobRepository batchJobRepository, BatchJobParser batchJobParser) {
        this.batchJobRepository = batchJobRepository;
        this.batchJobParser = batchJobParser;
        batchJobExecutors = new EnumMap<>(EngineKind.class);
        batchJobExecutors.put(EngineKind.JDBC, new JDBCBatchJobExecutor());
    }

    @Transactional
    public void save(BatchJob batchJob) {
        batchJobRepository.save(batchJob);
    }

    public boolean existsByJobName(String jobName) {
        return batchJobRepository.existsById(jobName);
    }

    public List<BatchJob> findNotCreatedStatusJobs() {
        return batchJobRepository.findByNotStatus(JobStatus.CREATED);
    }

    public BatchJob findByName(String jobName) {
        return batchJobRepository.findByName(jobName).orElse(null);
    }

    @Transactional
    public void deleteByJobName(String jobName) {
        batchJobRepository.deleteById(jobName);
    }

    public BatchJob parseJobByContent(String content) {
        return batchJobParser.parse(content);
    }

    public void executeJob(Date fireTime, BatchJob job) {
        BatchJobExecutor batchJobExecutor = requireNonNull(batchJobExecutors.get(job.getEngineKind()));
        batchJobExecutor.executeJob(fireTime, job, executePositionTracker);
    }

    public void executeJob(BatchJob job) {
        executeJob(null, job);
    }
}
