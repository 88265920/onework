package com.onework.core.service;

import com.onework.core.entity.StreamJob;
import com.onework.core.enums.JobStatus;
import com.onework.core.enums.ResumeMethod;
import com.onework.core.repository.StreamJobRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class StreamJobService {
    private StreamJobRepository streamJobRepository;

    @Autowired
    public StreamJobService(StreamJobRepository streamJobRepository) {
        this.streamJobRepository = streamJobRepository;
    }

    @Transactional
    public void save(StreamJob streamJob) {
        streamJobRepository.save(streamJob);
    }

    public boolean existsByJobName(String jobName) {
        return streamJobRepository.existsById(jobName);
    }

    public List<StreamJob> findNotCreatedStatusJobs() {
        return streamJobRepository.findByNotStatus(JobStatus.CREATED);
    }

    public StreamJob findByJobName(String jobName) {
        return streamJobRepository.findById(jobName).orElse(null);
    }

    public List<StreamJob> findByJobStatus(JobStatus jobStatus) {
        return streamJobRepository.findByStatus(jobStatus);
    }

    public String findIdByName(String jobName) {
        return streamJobRepository.findIdByName(jobName);
    }

    public JobStatus findStatusByName(String jobName) {
        return streamJobRepository.findStatusByName(jobName);
    }

    public Set<String> getUsedJobIds() {
        List<StreamJob> streamJobs = streamJobRepository.findByNotStatus(JobStatus.CREATED);
        Set<String> usedJobIds = new HashSet<>();
        for (StreamJob job : streamJobs) {
            if (StringUtils.isNotEmpty(job.getCheckpointJobId())) {
                usedJobIds.add(job.getCheckpointJobId());
            }
            if (StringUtils.isNotEmpty(job.getJobId())) {
                usedJobIds.add(job.getJobId());
            }
        }
        return usedJobIds;
    }

    @Transactional
    public void setStatusById(JobStatus jobStatus, String jobId) {
        streamJobRepository.setStatusById(jobStatus, jobId);
    }

    @Transactional
    public void setCheckpointJobIdById(String jobId) {
        streamJobRepository.setCheckpointJobIdById(jobId);
    }

    @Transactional
    public void setStatusAndIdByName(JobStatus jobStatus, String jobId, String jobName) {
        streamJobRepository.setStatusAndIdByName(jobStatus, jobId, jobName);
    }

    @Transactional
    public void setStatusAndSavepointById(JobStatus jobStatus, String savepoint, String jobName) {
        streamJobRepository.setStatusAndSavepointById(jobStatus, savepoint, jobName);
    }

    @Transactional
    public void setResumeMethodById(ResumeMethod resumeMethod, String jobId) {
        streamJobRepository.setResumeMethodById(resumeMethod, jobId);
    }

    @Transactional
    public void deleteByJobName(String jobName) {
        streamJobRepository.deleteById(jobName);
    }
}
