package com.onework.core.entity;

import com.onework.core.enums.ResumeMethod;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.*;
import java.util.List;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class StreamJob extends BaseJob {
    @OneToMany(cascade = {CascadeType.ALL}, mappedBy = "jobName", orphanRemoval = true)
    private List<SqlStatement> sqlStatements;

    private String jobId;

    private String checkpointJobId;

    private String savepoint;

    @Enumerated(value = EnumType.STRING)
    private ResumeMethod resumeMethod;
}
