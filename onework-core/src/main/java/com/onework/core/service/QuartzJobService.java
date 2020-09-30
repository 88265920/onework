package com.onework.core.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class QuartzJobService {
    private Scheduler scheduler;

    @SneakyThrows
    @Autowired
    public QuartzJobService(Scheduler scheduler) {
        this.scheduler = scheduler;
        scheduler.start();
    }

    @SneakyThrows
    public void addJob(Class<? extends Job> jobClass, String jobName, String jobGroupName, String cronTime,
                       Map<String, Object> jobData, boolean misFireDoNothing) {
        JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(jobName, jobGroupName).build();
        if (jobData != null && jobData.size() > 0) {
            jobDetail.getJobDataMap().putAll(jobData);
        }
        CronScheduleBuilder scheduleBuilder;
        if (misFireDoNothing) {
            scheduleBuilder = CronScheduleBuilder.cronSchedule(cronTime).withMisfireHandlingInstructionDoNothing();
        } else {
            scheduleBuilder = CronScheduleBuilder.cronSchedule(cronTime);
        }
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName, jobGroupName)
                .withSchedule(scheduleBuilder).build();
        scheduler.scheduleJob(jobDetail, trigger);
    }

    @SneakyThrows
    public void updateJob(String jobName, String jobGroupName, String cronTime, boolean misFireDoNothing) {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroupName);
        CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);
        CronScheduleBuilder scheduleBuilder;
        if (misFireDoNothing) {
            scheduleBuilder = CronScheduleBuilder.cronSchedule(cronTime).withMisfireHandlingInstructionDoNothing();
        } else {
            scheduleBuilder = CronScheduleBuilder.cronSchedule(cronTime);
        }
        trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();
        scheduler.rescheduleJob(triggerKey, trigger);
    }

    @SneakyThrows
    public void removeJob(String jobName, String jobGroupName) {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroupName);
        scheduler.pauseTrigger(triggerKey);
        scheduler.unscheduleJob(triggerKey);
        scheduler.deleteJob(JobKey.jobKey(jobName, jobGroupName));
    }

    @SneakyThrows
    public boolean existsJob(String jobName, String jobGroupName) {
        return scheduler.checkExists(new JobKey(jobName, jobGroupName));
    }

    @SneakyThrows
    public List<Map<String, Object>> queryAllJob() {
        List<Map<String, Object>> jobList = new ArrayList<>();
        GroupMatcher<JobKey> matcher = GroupMatcher.anyJobGroup();
        Set<JobKey> jobKeys = scheduler.getJobKeys(matcher);
        for (JobKey jobKey : jobKeys) {
            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
            for (Trigger trigger : triggers) {
                Map<String, Object> map = new HashMap<>();
                processJobData(jobList, jobKey, trigger, map);
            }
        }
        return jobList;
    }

    @SneakyThrows
    public List<Map<String, Object>> queryRunJob() {
        List<Map<String, Object>> jobList = new ArrayList<>();
        List<JobExecutionContext> executingJobs = scheduler.getCurrentlyExecutingJobs();
        for (JobExecutionContext executingJob : executingJobs) {
            Map<String, Object> jobData = new HashMap<>();
            JobDetail jobDetail = executingJob.getJobDetail();
            JobKey jobKey = jobDetail.getKey();
            Trigger trigger = executingJob.getTrigger();
            processJobData(jobList, jobKey, trigger, jobData);
        }
        return jobList;
    }

    private void processJobData(List<Map<String, Object>> jobList, JobKey jobKey, Trigger trigger,
                                Map<String, Object> jobData) throws SchedulerException {
        jobData.put("jobName", jobKey.getName());
        jobData.put("jobGroupName", jobKey.getGroup());
        jobData.put("description", "触发器:" + trigger.getKey());
        Trigger.TriggerState triggerState = scheduler.getTriggerState(trigger.getKey());
        jobData.put("jobStatus", triggerState.name());
        if (trigger instanceof CronTrigger) {
            CronTrigger cronTrigger = (CronTrigger) trigger;
            String cronExpression = cronTrigger.getCronExpression();
            jobData.put("jobTime", cronExpression);
        }
        jobList.add(jobData);
    }

    public Scheduler getScheduler() {
        return scheduler;
    }
}
