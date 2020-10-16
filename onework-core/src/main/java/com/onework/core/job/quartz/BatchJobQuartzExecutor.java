package com.onework.core.job.quartz;

import com.onework.core.ApplicationContextGetter;
import com.onework.core.entity.BatchJob;
import com.onework.core.service.BatchJobService;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.springframework.scheduling.quartz.QuartzJobBean;

import static java.util.Objects.requireNonNull;

@Slf4j
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class BatchJobQuartzExecutor extends QuartzJobBean {
    private BatchJobService batchJobService = ApplicationContextGetter.getContext().getBean(BatchJobService.class);

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        log.info("fire time: {}", context.getFireTime());
        String jobName = context.getJobDetail().getKey().getName();
        try {
            BatchJob batchJob = requireNonNull(batchJobService.findByName(jobName));
            batchJobService.executeJob(context.getFireTime(), batchJob);
        } catch (Exception e) {
            log.error("", e);
            JobExecutionException jee = new JobExecutionException(e);
            jee.setUnscheduleAllTriggers(true);
            throw jee;
        }
    }
}
