package com.onework.core.controller;

import com.google.common.io.ByteStreams;
import com.onework.core.entity.BatchJob;
import com.onework.core.enums.JobStatus;
import com.onework.core.job.quartz.BatchJobQuartzExecutor;
import com.onework.core.service.BatchJobService;
import com.onework.core.service.QuartzJobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping(value = "batchJob")
@SuppressWarnings("rawtypes")
public class BatchJobController {
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
    public Response create(@RequestParam("file") MultipartFile file) {
        if (file == null || file.isEmpty()) return Response.error("文件不存在或内容为空");

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            return Response.error("文件解析失败");
        }

        BatchJob batchJob;
        try {
            batchJob = batchJobService.parseJobByContent(content);
        } catch (Exception e) {
            return Response.error(e);
        }

        if (batchJobService.existsByJobName(batchJob.getJobName())) {
            return Response.error("任务已存在");
        } else {
            batchJob.setJobStatus(JobStatus.CREATED);
            try {
                batchJobService.executeJob(new Date(), batchJob);
                Map<String, Object> jobData = new HashMap<>();
                jobData.put("jobName", batchJob.getJobName());
                quartzJobService.addJob(BatchJobQuartzExecutor.class, batchJob.getJobName(), "batchJob",
                        batchJob.getCronTime(), jobData, false);
            } catch (Exception e) {
                return Response.error(e);
            }
        }

        return Response.ok();
    }
}
