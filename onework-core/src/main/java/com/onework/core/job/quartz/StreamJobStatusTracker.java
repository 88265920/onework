package com.onework.core.job.quartz;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.onework.core.ApplicationContextGetter;
import com.onework.core.client.FlinkRestClient;
import com.onework.core.enums.JobStatus;
import com.onework.core.service.StreamJobService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class StreamJobStatusTracker extends QuartzJobBean {
    private final StreamJobService streamJobService = ApplicationContextGetter.getContext().getBean(StreamJobService.class);
    private final FlinkRestClient flinkRestClient = ApplicationContextGetter.getContext().getBean(FlinkRestClient.class);

    @SneakyThrows
    @Override
    protected void executeInternal(JobExecutionContext context) {
        log.info("fire time: {}", context.getFireTime());
        JSONArray restJobs;
        restJobs = flinkRestClient.jobs();
        Map<String, JSONObject> restJobsMap = new HashMap<>();
        for (int i = 0; i < restJobs.size(); i++) {
            JSONObject obj = restJobs.getJSONObject(i);
            restJobsMap.put(obj.getString("id"), obj);
        }
        streamJobService.findByJobStatus(JobStatus.RUNNING).forEach(j -> {
            JSONObject restJob = restJobsMap.get(j.getJobId());
            if (restJob == null || !restJob.getString("status").equals("RUNNING")) {
                streamJobService.setStatusById(JobStatus.FAILED, j.getJobId());
            }
        });
    }
}
