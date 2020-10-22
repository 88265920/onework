package com.onework.core.entity;

import com.onework.core.converter.ListConverter;
import com.onework.core.enums.JobStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.*;
import java.util.List;

@Data
@MappedSuperclass
@EqualsAndHashCode(callSuper = true)
public abstract class BaseJob extends BaseEntity {
    @Id
    @NonNull
    private String jobName;

    @NonNull
    @Enumerated(value = EnumType.STRING)
    private JobStatus jobStatus;

    @NonNull
    @OneToOne(cascade = {CascadeType.ALL}, mappedBy = "jobName", orphanRemoval = true)
    private JobEntry jobEntry;

    @NonNull
    @OneToMany(cascade = {CascadeType.ALL}, mappedBy = "jobName", orphanRemoval = true)
    private List<SqlStatement> sqlStatements;

    @Lob
    @Column(columnDefinition = "text")
    @Convert(converter = ListConverter.class)
    private List<String> dependentJobNames;

    public BaseJob() {
    }
}
