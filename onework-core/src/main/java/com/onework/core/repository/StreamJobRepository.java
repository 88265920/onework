package com.onework.core.repository;

import com.onework.core.common.Constants;
import com.onework.core.entity.StreamJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StreamJobRepository extends JpaRepository<StreamJob, String> {
    @Query("select j from StreamJob j where j.jobStatus = ?1")
    List<StreamJob> findByStatus(Constants.JobStatus jobStatus);

    @Query("select j from StreamJob j where j.jobStatus <> ?1")
    List<StreamJob> findByNotStatus(Constants.JobStatus jobStatus);

    @Query("select j.jobId from StreamJob j where j.jobName = ?1")
    String findIdByName(String jobName);

    @Query("select j.jobStatus from StreamJob j where j.jobName = ?1")
    Constants.JobStatus findStatusByName(String jobName);

    @Modifying
    @Query("update StreamJob j set j.jobStatus = ?1 where j.jobId = ?2")
    void setStatusById(Constants.JobStatus jobStatus, String jobId);

    @Modifying
    @Query("update StreamJob j set j.checkpointJobId = ?1 where j.jobId = ?1")
    void setCheckpointJobIdById(String jobId);

    @Modifying
    @Query("update StreamJob  j set j.jobStatus = ?1, j.jobId = ?2 where j.jobName = ?3")
    void setStatusAndIdByName(Constants.JobStatus jobStatus, String jobId, String jobName);

    @Modifying
    @Query("update StreamJob j set j.jobStatus = ?1, j.savepoint = ?2 where j.jobId = ?3")
    void setStatusAndSavepointById(Constants.JobStatus jobStatus, String savepoint, String jobId);

    @Modifying
    @Query("update StreamJob j set j.resumeMethod = ?1 where j.jobId = ?2")
    void setResumeMethodById(Constants.ResumeMethod resumeMethod, String jobId);
}
