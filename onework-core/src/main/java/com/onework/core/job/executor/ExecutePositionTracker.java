package com.onework.core.job.executor;

import java.util.Date;

/**
 * @author kangj
 * @date 2020/11/20
 **/
public interface ExecutePositionTracker {
    void executePosition(String jobName, Date fireTime, int pos);
}
