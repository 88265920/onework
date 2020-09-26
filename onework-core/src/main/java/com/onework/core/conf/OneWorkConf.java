package com.onework.core.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "onework")
public class OneWorkConf {
    private Flink flink;
    private Yarn yarn;
    private Hdfs hdfs;

    @Data
    public static class Flink {
        private String checkpoint;
        private String savepoint;
    }

    @Data
    public static class Yarn {
        private String queue;
        private String sessionAppName;
        private String siteFile;
    }

    @Data
    public static class Hdfs {
        private String fileOwner;
        private String coreSiteFile;
        private String hdfsSiteFile;
    }
}
