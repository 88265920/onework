package com.onework.core.job.quartz;

import com.onework.core.ApplicationContextGetter;
import com.onework.core.client.HdfsCheckpointManager;
import com.onework.core.entity.StreamJob;
import com.onework.core.enums.ResumeMethod;
import com.onework.core.service.StreamJobService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.quartz.JobExecutionContext;

import java.util.List;
import java.util.Set;

@Slf4j
public class StreamJobCheckpointCleaner extends QuartzJobBean {
    private StreamJobService streamJobService = ApplicationContextGetter.getContext().getBean(StreamJobService.class);
    private HdfsCheckpointManager hdfsCheckPointManager = ApplicationContextGetter.getContext().getBean(HdfsCheckpointManager.class);

    @SneakyThrows
    @Override
    protected void executeInternal(JobExecutionContext context) {
        FileSystem fs = hdfsCheckPointManager.getFs();
        List<StreamJob> streamJobs = streamJobService.findNotCreatedStatusJobs();
        for (StreamJob job : streamJobs) {
            String savepoint = job.getSavepoint();
            if (!job.getResumeMethod().equals(ResumeMethod.SAVEPOINT) && StringUtils.isNotEmpty(savepoint)) {
                fs.delete(new Path(savepoint), true);
                log.info("Clear expired savepoint directory: {}", savepoint);
            }
        }
        Set<String> usedJobIds = streamJobService.getUsedJobIds();
        try {
            hdfsCheckPointManager.clearExpiredCheckpoint(usedJobIds);
        } finally {
            hdfsCheckPointManager.close();
        }
    }
}
