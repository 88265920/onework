package com.onework.core.job.executor;

import com.onework.core.entity.BatchJob;
import lombok.NonNull;

import java.util.Date;

/**
 * @author kangj
 * @date 2020/11/20
 **/
public interface BatchJobExecutor {
    void executeJob(Date fireTime, @NonNull BatchJob batchJob, @NonNull ExecutePositionTracker tracker);
}
