package com.onework.core.job.executor;

import com.onework.core.entity.BatchJob;
import lombok.NonNull;

import java.util.Date;

public interface BatchJobExecutor {
    void executeJob(Date fireTime, @NonNull BatchJob batchJob, @NonNull ExecutePositionTracker tracker);
}
