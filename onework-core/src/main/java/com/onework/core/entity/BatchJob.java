package com.onework.core.entity;

import com.onework.core.enums.EngineKind;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.OneToMany;
import java.util.Date;
import java.util.List;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class BatchJob extends BaseJob {
    @OneToMany(cascade = {CascadeType.ALL}, mappedBy = "jobName", orphanRemoval = true)
    private List<StreamSqlStatement> streamSqlStatements;

    private EngineKind engineKind;

    private String jobId;

    private String cronTime;

    private Date fireTime;

    private Integer executePosition;
}
