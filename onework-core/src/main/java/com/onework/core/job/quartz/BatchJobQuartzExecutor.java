package com.onework.core.job.quartz;

import com.onework.core.ApplicationContextGetter;
import com.onework.core.entity.BatchJob;
import com.onework.core.service.BatchJobService;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BatchJobQuartzExecutor extends QuartzJobBean {
    private BatchJobService batchJobService = ApplicationContextGetter.getContext().getBean(BatchJobService.class);

    @Override
    protected void executeInternal(JobExecutionContext context) {
        System.out.println(context.getFireTime());
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        String jobName = jobDataMap.getString("jobName");
        checkArgument(StringUtils.isNotEmpty(jobName));
        BatchJob batchJob = batchJobService.findByName(jobName).orElse(null);
        checkNotNull(batchJob);
        batchJobService.executeJob(context.getFireTime(), batchJob);
    }
}
