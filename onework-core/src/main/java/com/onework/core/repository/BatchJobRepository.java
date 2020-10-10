package com.onework.core.repository;

import com.onework.core.entity.BatchJob;
import com.onework.core.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@Repository
public interface BatchJobRepository extends JpaRepository<BatchJob, String> {
    @Query("select j from BatchJob j where j.jobStatus <> ?1")
    List<BatchJob> findByNotStatus(JobStatus jobStatus);

    @Query("select j from BatchJob j where j.jobName = ?1")
    Optional<BatchJob> findByName(String jobName);

    @Modifying
    @Query("update BatchJob  j set j.fireTime = ?1, j.executePosition = ?2 where j.jobName = ?3")
    void setFireTimeAndExecutePositionByName(Date fireTime, Integer executePosition, String jobName);
}
