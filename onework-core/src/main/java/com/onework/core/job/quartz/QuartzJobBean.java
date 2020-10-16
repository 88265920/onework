package com.onework.core.job.quartz;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyAccessorFactory;

@Slf4j
public abstract class QuartzJobBean implements Job {

    public final void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            BeanWrapper bw = PropertyAccessorFactory.forBeanPropertyAccess(this);
            MutablePropertyValues pvs = new MutablePropertyValues();
            pvs.addPropertyValues(context.getScheduler().getContext());
            pvs.addPropertyValues(context.getMergedJobDataMap());
            bw.setPropertyValues(pvs, true);
        } catch (SchedulerException e) {
            throw new JobExecutionException(e);
        }

        log.info("Execute the scheduled task, description={}, jobData={}", context.getJobDetail().getDescription(),
                new JSONObject(context.getJobDetail().getJobDataMap()).toJSONString());
        executeInternal(context);
        log.info("Successfully execute the scheduled task, description={}", context.getJobDetail().getDescription());
    }

    protected abstract void executeInternal(JobExecutionContext context);
}
