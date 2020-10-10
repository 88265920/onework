package com.onework.core.job.executor;

import java.util.Date;

public interface ExecutePositionTracker {
    void executePosition(String jobName, Date fireTime, int pos);
}
