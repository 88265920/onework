package com.onework.core;

import com.onework.core.conf.OneWorkConf;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@SpringBootApplication
@EnableConfigurationProperties(value = {OneWorkConf.class})
public class OneWorkCoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(OneWorkCoreApplication.class, args);
    }
}
