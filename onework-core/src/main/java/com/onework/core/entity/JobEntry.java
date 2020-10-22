package com.onework.core.entity;

import com.onework.core.converter.MapConverter;
import com.onework.core.enums.JobKind;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.*;
import java.util.Map;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class JobEntry extends BaseEntity {
    @Id
    @NonNull
    private String jobName;

    @Enumerated(value = EnumType.STRING)
    @NonNull
    private JobKind jobKind;

    @Lob
    @Column(columnDefinition = "text")
    @Convert(converter = MapConverter.class)
    @NonNull
    private Map<String, String> jobParams;

    public JobEntry() {
    }

    public JobEntry(String jobName, @NonNull JobKind jobKind, @NonNull Map<String, String> jobParams) {
        this.jobName = jobName;
        this.jobKind = jobKind;
        this.jobParams = jobParams;
    }
}
