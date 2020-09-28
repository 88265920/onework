package com.onework.core.controller;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.onework.core.client.FlinkRestClient;
import com.onework.core.client.HdfsCheckpointManager;
import com.onework.core.entity.StreamJob;
import com.onework.core.enums.JobStatus;
import com.onework.core.enums.ResumeMethod;
import com.onework.core.job.executor.StreamJobExecutor;
import com.onework.core.job.parser.StreamJobParser;
import com.onework.core.job.quartz.StreamJobCheckpointCleaner;
import com.onework.core.job.quartz.StreamJobStatusTracker;
import com.onework.core.service.QuartzJobService;
import com.onework.core.service.StreamJobService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
@RestController
@RequestMapping(path = "streamJob")
@SuppressWarnings("rawtypes")
public class StreamJobController {
    private StreamJobService streamJobService;
    private StreamJobParser streamJobParser;
    private StreamJobExecutor streamJobExecutor;
    private FlinkRestClient flinkRestClient;
    private HdfsCheckpointManager hdfsCheckPointManager;
    private QuartzJobService quartzJobService;

    @Autowired
    public StreamJobController(StreamJobService streamJobService, StreamJobParser streamJobParser,
                               StreamJobExecutor streamJobExecutor, FlinkRestClient flinkRestClient,
                               HdfsCheckpointManager hdfsCheckPointManager, QuartzJobService quartzJobService) {
        this.streamJobService = streamJobService;
        this.streamJobParser = streamJobParser;
        this.streamJobExecutor = streamJobExecutor;
        this.flinkRestClient = flinkRestClient;
        this.hdfsCheckPointManager = hdfsCheckPointManager;
        this.quartzJobService = quartzJobService;

        createQuartzSystemJob();
    }

    private void createQuartzSystemJob() {
        String jobGroupName = "system";
        String jobName = "streamJobStatusTracker";
        if (!quartzJobService.existsJob(jobName, jobGroupName)) {
            // 每5分钟执行一次实时任务状态检测
            quartzJobService.addJob(StreamJobStatusTracker.class, jobName, jobGroupName, "0 0/5 * * * ?",
                    Maps.newHashMap());
        }

        jobName = "streamJobCheckpointCleaner";
        if (!quartzJobService.existsJob(jobName, jobGroupName)) {
            // 每天凌晨2点清理无效的checkpoint/savepoint
            quartzJobService.addJob(StreamJobCheckpointCleaner.class, jobName, jobGroupName, "0 0 2 * * ? ",
                    Maps.newHashMap());
        }
    }

    @GetMapping("jobs")
    @ResponseBody
    public Response jobs() {
        return Response.data(streamJobService.findNotCreatedStatusJobs());
    }

    @PostMapping("create")
    @ResponseBody
    @Transactional
    public Response create(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) return Response.error("文件不存在");

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            log.error("", e);
            return Response.error("文件解析失败");
        }

        StreamJob streamJob = streamJobParser.parse(content);
        if (streamJobService.existsByJobName(streamJob.getJobName())) {
            return Response.error("任务已存在");
        } else {
            streamJob.setJobStatus(JobStatus.CREATED);
            streamJobExecutor.executeJob(streamJob);
        }

        return Response.ok();
    }

    @PostMapping("upgrade")
    @ResponseBody
    @Transactional
    public Response upgrade(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) return Response.error("文件不存在");

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            log.error("", e);
            return Response.error("文件解析失败");
        }

        StreamJob streamJob = streamJobParser.parse(content);
        StreamJob oldStreamJob = streamJobService.findByJobName(streamJob.getJobName());
        if (oldStreamJob == null) return Response.error("任务不存在");

        streamJob.setCheckpointJobId(oldStreamJob.getCheckpointJobId());
        streamJob.setSavepoint(oldStreamJob.getSavepoint());
        streamJob.setJobStatus(JobStatus.CREATED);

        ResumeMethod resumeMethod = oldStreamJob.getResumeMethod();
        if (resumeMethod.equals(ResumeMethod.CHECKPOINT)) {
            streamJobExecutor.executeJob(streamJob);
        } else if (resumeMethod.equals(ResumeMethod.SAVEPOINT)) {
            streamJobExecutor.executeJobWithSavepoint(streamJob);
        }

        return Response.ok();
    }

    @GetMapping("resume")
    @ResponseBody
    @Transactional
    public Response resume(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error("任务不存在");

        JobStatus jobStatus = streamJobService.findStatusByName(jobName);
        if (jobStatus.equals(JobStatus.SUSPEND) || jobStatus.equals(JobStatus.FAILED)) {
            StreamJob streamJob = streamJobService.findByJobName(jobName);
            ResumeMethod resumeMethod = streamJob.getResumeMethod();
            if (resumeMethod.equals(ResumeMethod.CHECKPOINT)) {
                streamJobExecutor.executeJob(streamJob);
            } else if (resumeMethod.equals(ResumeMethod.SAVEPOINT)) {
                streamJobExecutor.executeJobWithSavepoint(streamJob);
            }

            return Response.ok();
        }

        return Response.error("任务恢复失败");
    }

    @GetMapping("resumeWithoutState")
    @ResponseBody
    @Transactional
    public Response resumeWithoutState(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error("任务不存在");

        JobStatus jobStatus = streamJobService.findStatusByName(jobName);
        if (jobStatus.equals(JobStatus.SUSPEND) || jobStatus.equals(JobStatus.FAILED)) {
            StreamJob streamJob = streamJobService.findByJobName(jobName);
            streamJobExecutor.executeJobWithoutState(streamJob);
            return Response.ok();
        }

        return Response.error("任务恢复失败");
    }

    @GetMapping("suspend")
    @ResponseBody
    @Transactional
    public Response suspend(@NonNull String jobName) throws IOException {
        if (!streamJobService.existsByJobName(jobName)) return Response.error("任务不存在");

        String jobId = streamJobService.findIdByName(jobName);
        checkState(StringUtils.isNotEmpty(jobId));
        if (!hdfsCheckPointManager.hasCheckPoint(jobId)) return Response.error("没有Checkpoint信息");
        boolean success = flinkRestClient.suspendJob(jobId);
        if (!success) return Response.error("任务暂停失败");
        streamJobService.setStatusById(JobStatus.SUSPEND, jobId);
        streamJobService.setCheckpointJobIdById(jobId);
        streamJobService.setResumeMethodById(ResumeMethod.CHECKPOINT, jobId);

        return Response.ok();
    }

    @GetMapping("suspendWithSavepoint")
    @ResponseBody
    @Transactional
    public Response suspendWithSavepoint(@NonNull String jobName) throws IOException {
        if (!streamJobService.existsByJobName(jobName)) return Response.error("任务不存在");

        String jobId = streamJobService.findIdByName(jobName);
        checkState(StringUtils.isNotEmpty(jobId));
        if (!hdfsCheckPointManager.hasCheckPoint(jobId)) return Response.error("没有Checkpoint信息");
        try {
            String savepoint = flinkRestClient.suspendJobWithSavepoint(jobId);
            log.info("suspend savepoint = ".concat(savepoint));
            streamJobService.setStatusAndSavepointById(JobStatus.SUSPEND, savepoint, jobId);
            streamJobService.setResumeMethodById(ResumeMethod.SAVEPOINT, jobId);
        } catch (InterruptedException e) {
            return Response.error("任务暂停失败");
        }

        return Response.ok();
    }

    @GetMapping("delete")
    @ResponseBody
    @Transactional
    public Response delete(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error("任务不存在");
        streamJobService.deleteByJobName(jobName);
        return Response.ok();
    }
}
