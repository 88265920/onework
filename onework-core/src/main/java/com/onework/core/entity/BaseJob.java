package com.onework.core.entity;

import com.onework.core.converter.ListConverter;
import com.onework.core.converter.MapConverter;
import com.onework.core.enums.JobKind;
import com.onework.core.enums.JobStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.*;
import java.util.List;
import java.util.Map;

/**
 * @author kangj
 * @date 2020/11/20
 **/
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
    @Enumerated(value = EnumType.STRING)
    private JobKind jobKind;

    @Lob
    @NonNull
    @Column(columnDefinition = "text")
    @Convert(converter = MapConverter.class)
    private Map<String, String> jobArguments;

    @Lob
    @Column(columnDefinition = "text")
    @Convert(converter = ListConverter.class)
    private List<String> dependentJobNames;

    public BaseJob() {
    }
}
