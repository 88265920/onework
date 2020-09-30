package com.onework.core.entity;

import com.onework.core.enums.EngineKind;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.Entity;
import java.util.Date;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class BatchJob extends BaseJob {
    private EngineKind engineKind;

    private String jobId;

    private String cronTime;

    private Date fireTime;

    private Integer executePosition;
}
