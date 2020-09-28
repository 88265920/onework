package com.onework.core.entity;

import com.onework.core.enums.ResumeMethod;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class StreamJob extends BaseJob {
    private String jobId;

    private String checkpointJobId;

    private String savepoint;

    @Enumerated(value = EnumType.STRING)
    private ResumeMethod resumeMethod;
}
