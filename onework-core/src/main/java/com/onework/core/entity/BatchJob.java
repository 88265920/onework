package com.onework.core.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.Entity;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class BatchJob extends BaseJob {
    private String jobId;

    private String cronTime;
}
