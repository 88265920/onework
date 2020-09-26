package com.onework.core.job.quartz;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.onework.core.ApplicationContextGetter;
import com.onework.core.client.FlinkRestClient;
import com.onework.core.common.Constants;
import com.onework.core.service.StreamJobService;
import lombok.SneakyThrows;
import org.quartz.JobExecutionContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.util.HashMap;
import java.util.Map;

public class StreamJobStatusTracker extends QuartzJobBean {
    private StreamJobService streamJobService = ApplicationContextGetter.getContext().getBean(StreamJobService.class);
    private FlinkRestClient flinkRestClient = ApplicationContextGetter.getContext().getBean(FlinkRestClient.class);

    @SneakyThrows
    @Override
    protected void executeInternal(JobExecutionContext context) {
        System.out.println(context.getFireTime());
        JSONArray restJobs;
        restJobs = flinkRestClient.jobs();
        Map<String, JSONObject> restJobsMap = new HashMap<>();
        for (int i = 0; i < restJobs.size(); i++) {
            JSONObject obj = restJobs.getJSONObject(i);
            restJobsMap.put(obj.getString("id"), obj);
        }
        streamJobService.findByJobStatus(Constants.JobStatus.RUNNING).forEach(j -> {
            JSONObject restJob = restJobsMap.get(j.getJobId());
            if (restJob == null || !restJob.getString("status").equals("RUNNING")) {
                streamJobService.setStatusById(Constants.JobStatus.FAILED, j.getJobId());
            }
        });
    }
}
