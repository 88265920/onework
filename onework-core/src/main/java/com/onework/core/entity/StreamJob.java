package com.onework.core.entity;

import com.onework.core.enums.ResumeMethod;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.*;
import java.util.List;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class StreamJob extends BaseJob {
    @OneToMany(cascade = {CascadeType.ALL}, mappedBy = "jobName", orphanRemoval = true)
    private List<StreamSqlStatement> streamSqlStatements;

    private String jobId;

    private String checkpointJobId;

    private String savepoint;

    @Enumerated(value = EnumType.STRING)
    private ResumeMethod resumeMethod;
}
