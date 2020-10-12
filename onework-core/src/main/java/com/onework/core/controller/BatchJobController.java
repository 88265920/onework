package com.onework.core.controller;

import com.google.common.io.ByteStreams;
import com.onework.core.entity.BatchJob;
import com.onework.core.enums.JobStatus;
import com.onework.core.job.quartz.BatchJobQuartzExecutor;
import com.onework.core.service.BatchJobService;
import com.onework.core.service.QuartzJobService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.onework.core.common.JobErrorMsg.*;

@Slf4j
@RestController
@RequestMapping(value = "batchJob")
@SuppressWarnings("rawtypes")
public class BatchJobController {
    private static final String QUARTZ_JOB_GROUP_NAME = "onework-batch-job";
    private BatchJobService batchJobService;
    private QuartzJobService quartzJobService;

    @Autowired
    public BatchJobController(BatchJobService batchJobService, QuartzJobService quartzJobService) {
        this.batchJobService = batchJobService;
        this.quartzJobService = quartzJobService;
    }

    @GetMapping("jobs")
    @ResponseBody
    public Response jobs() {
        return Response.data(batchJobService.findNotCreatedStatusJobs());
    }

    @PostMapping("create")
    @ResponseBody
    @Transactional
    public Response create(@NonNull MultipartFile file) {
        if (file.isEmpty()) return Response.error(FILE_NOT_EXIST_OR_EMPTY);

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            return Response.error(FILE_PARSING_FAILED);
        }

        BatchJob batchJob;
        try {
            batchJob = batchJobService.parseJobByContent(content);
        } catch (Exception e) {
            return Response.error(e);
        }

        if (batchJobService.existsByJobName(batchJob.getJobName())) {
            return Response.error(JOB_EXISTED);
        } else {
            batchJob.setJobStatus(JobStatus.CREATED);
            try {
                batchJobService.executeJob(new Date(), batchJob);
                Map<String, Object> jobData = new HashMap<>();
                jobData.put("jobName", batchJob.getJobName());
                quartzJobService.addJob(BatchJobQuartzExecutor.class, batchJob.getJobName(), QUARTZ_JOB_GROUP_NAME,
                        batchJob.getCronTime(), jobData);
                batchJob.setJobStatus(JobStatus.RUNNING);
                batchJobService.save(batchJob);
            } catch (Exception e) {
                return Response.error(e);
            }
        }

        return Response.ok();
    }

    @PostMapping("upgrade")
    @ResponseBody
    @Transactional
    public Response upgrade(@NonNull MultipartFile file) {
        if (file.isEmpty()) return Response.error(FILE_NOT_EXIST_OR_EMPTY);

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            return Response.error(FILE_PARSING_FAILED);
        }

        BatchJob batchJob;
        try {
            batchJob = batchJobService.parseJobByContent(content);
        } catch (Exception e) {
            return Response.error(e);
        }

        BatchJob oldBatchJob = batchJobService.findByName(batchJob.getJobName());
        if (oldBatchJob == null) return Response.error(JOB_NOT_FOUND);

        batchJob.setFireTime(oldBatchJob.getFireTime());
        batchJob.setJobId(oldBatchJob.getJobId());
        batchJob.setExecutePosition(oldBatchJob.getExecutePosition());

        try {
            checkState(quartzJobService.existsJob(batchJob.getJobName(), QUARTZ_JOB_GROUP_NAME));
            quartzJobService.updateJob(batchJob.getJobName(), QUARTZ_JOB_GROUP_NAME, batchJob.getCronTime());
            batchJob.setJobStatus(JobStatus.RUNNING);
            batchJobService.save(batchJob);
        } catch (Exception e) {
            return Response.error(e);
        }

        return Response.ok();
    }

    @GetMapping("delete")
    @ResponseBody
    @Transactional
    public Response delete(@NonNull String jobName) {
        if (!batchJobService.existsByJobName(jobName)) return Response.error(JOB_NOT_FOUND);
        batchJobService.deleteByJobName(jobName);
        return Response.ok();
    }
}
