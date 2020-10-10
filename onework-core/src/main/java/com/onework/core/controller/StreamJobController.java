package com.onework.core.controller;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.onework.core.entity.StreamJob;
import com.onework.core.enums.JobStatus;
import com.onework.core.enums.ResumeMethod;
import com.onework.core.job.executor.StreamJobExecutor;
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
    private static final String JOB_NOT_FOUND = "任务不存在";
    private StreamJobService streamJobService;
    private StreamJobExecutor streamJobExecutor;
    private QuartzJobService quartzJobService;

    @Autowired
    public StreamJobController(StreamJobService streamJobService, StreamJobExecutor streamJobExecutor,
                               QuartzJobService quartzJobService) {
        this.streamJobService = streamJobService;
        this.streamJobExecutor = streamJobExecutor;
        this.quartzJobService = quartzJobService;
    }

    @GetMapping("create_system_jobs")
    @ResponseBody
    public Response createSystemJobs() {
        try {
            createQuartzSystemJob();
        } catch (Exception e) {
            return Response.error(e);
        }
        return Response.ok();
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
        if (file == null || file.isEmpty()) return Response.error("文件不存在或内容为空");

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            return Response.error("文件解析失败");
        }

        StreamJob streamJob;
        try {
            streamJob = streamJobService.parseJobByContent(content);
        } catch (Exception e) {
            return Response.error(e);
        }

        if (streamJobService.existsByJobName(streamJob.getJobName())) {
            return Response.error("任务已存在");
        } else {
            streamJob.setJobStatus(JobStatus.CREATED);
            try {
                streamJobExecutor.executeJob(streamJob);
            } catch (Exception e) {
                return Response.error(e);
            }
        }

        return Response.ok();
    }

    @PostMapping("upgrade")
    @ResponseBody
    @Transactional
    public Response upgrade(@RequestParam("file") MultipartFile file) {
        if (file == null || file.isEmpty()) return Response.error("文件不存在或内容为空");

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            return Response.error("文件解析失败");
        }

        StreamJob streamJob;
        try {
            streamJob = streamJobService.parseJobByContent(content);
        } catch (Exception e) {
            return Response.error(e);
        }
        StreamJob oldStreamJob = streamJobService.findByJobName(streamJob.getJobName());
        if (oldStreamJob == null) return Response.error(JOB_NOT_FOUND);

        streamJob.setCheckpointJobId(oldStreamJob.getCheckpointJobId());
        streamJob.setSavepoint(oldStreamJob.getSavepoint());
        streamJob.setResumeMethod(oldStreamJob.getResumeMethod());
        streamJob.setJobStatus(JobStatus.CREATED);

        return resumeJob(streamJob, oldStreamJob);
    }

    @GetMapping("resume")
    @ResponseBody
    @Transactional
    public Response resume(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error(JOB_NOT_FOUND);

        StreamJob streamJob = streamJobService.findByName(jobName);
        if (streamJob == null) {
            return Response.error(JOB_NOT_FOUND);
        }
        JobStatus jobStatus = streamJob.getJobStatus();
        if (jobStatus.equals(JobStatus.SUSPEND) || jobStatus.equals(JobStatus.FAILED)) {
            return resumeJob(streamJob, streamJob);
        }

        return Response.error("任务恢复失败");
    }

    @GetMapping("resumeWithoutState")
    @ResponseBody
    @Transactional
    public Response resumeWithoutState(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error(JOB_NOT_FOUND);

        StreamJob streamJob = streamJobService.findByName(jobName);
        if (streamJob == null) {
            return Response.error(JOB_NOT_FOUND);
        }

        JobStatus jobStatus = streamJob.getJobStatus();
        if (jobStatus.equals(JobStatus.SUSPEND) || jobStatus.equals(JobStatus.FAILED)) {
            try {
                streamJobExecutor.executeJobWithoutState(streamJob);
            } catch (Exception e) {
                return Response.error(e);
            }
            return Response.ok();
        }

        return Response.error("任务恢复失败");
    }

    @GetMapping("suspend")
    @ResponseBody
    @Transactional
    public Response suspend(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error(JOB_NOT_FOUND);

        StreamJob streamJob = streamJobService.findByName(jobName);
        if (streamJob == null) {
            return Response.error(JOB_NOT_FOUND);
        }
        String jobId = streamJob.getJobId();
        checkState(StringUtils.isNotEmpty(jobId));
        if (!streamJobService.hasCheckPoint(jobId)) return Response.error("没有Checkpoint信息");
        boolean success;
        try {
            success = streamJobService.suspendJob(jobId);
        } catch (Exception e) {
            return Response.error(e);
        }
        if (!success) return Response.error("任务暂停失败");
        streamJobService.setStatusById(JobStatus.SUSPEND, jobId);
        streamJobService.setCheckpointJobIdById(jobId);
        streamJobService.setResumeMethodById(ResumeMethod.CHECKPOINT, jobId);

        return Response.ok();
    }

    @GetMapping("suspendWithSavepoint")
    @ResponseBody
    @Transactional
    public Response suspendWithSavepoint(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error(JOB_NOT_FOUND);

        StreamJob streamJob = streamJobService.findByName(jobName);
        if (streamJob == null) {
            return Response.error(JOB_NOT_FOUND);
        }
        String jobId = streamJob.getJobId();
        checkState(StringUtils.isNotEmpty(jobId));
        if (!streamJobService.hasCheckPoint(jobId)) return Response.error("没有Checkpoint信息");
        try {
            String savepoint = streamJobService.suspendJobWithSavepoint(jobId);
            log.info("suspend savepoint = ".concat(savepoint));
            streamJobService.setStatusAndSavepointById(JobStatus.SUSPEND, savepoint, jobId);
            streamJobService.setResumeMethodById(ResumeMethod.SAVEPOINT, jobId);
        } catch (Exception e) {
            return Response.error(e);
        }

        return Response.ok();
    }

    @GetMapping("delete")
    @ResponseBody
    @Transactional
    public Response delete(@NonNull String jobName) {
        if (!streamJobService.existsByJobName(jobName)) return Response.error(JOB_NOT_FOUND);
        streamJobService.deleteByJobName(jobName);
        return Response.ok();
    }

    private Response resumeJob(StreamJob streamJob, StreamJob oldStreamJob) {
        try {
            ResumeMethod resumeMethod = oldStreamJob.getResumeMethod();
            if (resumeMethod.equals(ResumeMethod.CHECKPOINT)) {
                streamJobExecutor.executeJob(streamJob);
            } else if (resumeMethod.equals(ResumeMethod.SAVEPOINT)) {
                streamJobExecutor.executeJobWithSavepoint(streamJob);
            }
        } catch (Exception e) {
            return Response.error(e);
        }

        return Response.ok();
    }

    private void createQuartzSystemJob() {
        String jobGroupName = "system";
        String jobName = "streamJobStatusTracker";
        if (!quartzJobService.existsJob(jobName, jobGroupName)) {
            // 每5分钟执行一次实时任务状态检测
            quartzJobService.addJob(StreamJobStatusTracker.class, jobName, jobGroupName, "0 0/5 * * * ?",
                    Maps.newHashMap(), true);
        }

        jobName = "streamJobCheckpointCleaner";
        if (!quartzJobService.existsJob(jobName, jobGroupName)) {
            // 每天凌晨2点清理无效的checkpoint/savepoint
            quartzJobService.addJob(StreamJobCheckpointCleaner.class, jobName, jobGroupName, "0 0 2 * * ? ",
                    Maps.newHashMap(), true);
        }
    }
}
