package com.onework.core.entity;

import com.onework.core.common.Constants;
import com.onework.core.converter.List2StringConverter;
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

    @Enumerated(value = EnumType.STRING)
    @NonNull
    private Constants.JobStatus jobStatus;

    @NonNull
    @JoinColumn(name = "jobName")
    @OneToOne(cascade = {CascadeType.ALL}, fetch = FetchType.EAGER, orphanRemoval = true)
    private JobEntry jobEntry;

    @OneToMany(cascade = {CascadeType.ALL}, fetch = FetchType.EAGER, orphanRemoval = true)
    @JoinColumn(name = "jobName")
    @NonNull
    private List<SqlStatement> sqlStatements;

    @Lob
    @Column(columnDefinition = "text")
    @Convert(converter = List2StringConverter.class)
    private List<String> dependentJobNames;

    public BaseJob() {
    }
}
