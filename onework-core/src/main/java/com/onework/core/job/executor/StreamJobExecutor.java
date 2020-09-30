package com.onework.core.job.executor;

import com.onework.core.entity.StreamJob;
import lombok.NonNull;

public interface StreamJobExecutor {
    void executeJob(@NonNull StreamJob job);

    void executeJobWithSavepoint(@NonNull StreamJob job);

    void executeJobWithoutState(@NonNull StreamJob job);
}
