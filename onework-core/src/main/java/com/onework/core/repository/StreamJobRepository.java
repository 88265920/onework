package com.onework.core.repository;

import com.onework.core.entity.StreamJob;
import com.onework.core.enums.JobStatus;
import com.onework.core.enums.ResumeMethod;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Repository
public interface StreamJobRepository extends JpaRepository<StreamJob, String> {
    @Query("select j from StreamJob j where j.jobStatus = ?1")
    List<StreamJob> findByStatus(JobStatus jobStatus);

    @Query("select j from StreamJob j where j.jobStatus <> ?1")
    List<StreamJob> findByNotStatus(JobStatus jobStatus);

    @Query("select j from StreamJob j where j.jobName = ?1")
    Optional<StreamJob> findByName(String jobName);

    @Modifying
    @Query("update StreamJob j set j.jobStatus = ?1 where j.jobId = ?2")
    void setStatusById(JobStatus jobStatus, String jobId);

    @Modifying
    @Query("update StreamJob j set j.checkpointJobId = ?1 where j.jobId = ?1")
    void setCheckpointJobIdById(String jobId);

    @Modifying
    @Query("update StreamJob  j set j.jobStatus = ?1, j.jobId = ?2 where j.jobName = ?3")
    void setStatusAndIdByName(JobStatus jobStatus, String jobId, String jobName);

    @Modifying
    @Query("update StreamJob j set j.jobStatus = ?1, j.savepoint = ?2 where j.jobId = ?3")
    void setStatusAndSavepointById(JobStatus jobStatus, String savepoint, String jobId);

    @Modifying
    @Query("update StreamJob j set j.resumeMethod = ?1 where j.jobId = ?2")
    void setResumeMethodById(ResumeMethod resumeMethod, String jobId);
}
