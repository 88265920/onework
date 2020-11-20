package com.onework.core.job.executor;

import com.onework.core.entity.StreamJob;
import lombok.NonNull;

/**
 * @author kangj
 * @date 2020/11/20
 **/
public interface StreamJobExecutor {
    void executeJob(@NonNull StreamJob job);

    void executeJobWithSavepoint(@NonNull StreamJob job);

    void executeJobWithoutState(@NonNull StreamJob job);
}
