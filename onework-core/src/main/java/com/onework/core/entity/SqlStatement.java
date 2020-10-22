package com.onework.core.entity;

import com.onework.core.enums.JobKind;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.*;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class SqlStatement extends BasePKEntity {

    @NonNull
    private String jobName;

    @NonNull
    @Enumerated(value = EnumType.STRING)
    private JobKind jobKind;

    @Lob
    @Column(columnDefinition = "text")
    @NonNull
    private String sqlContent;

    public SqlStatement() {
    }

    public SqlStatement(@NonNull String jobName, @NonNull JobKind jobKind, @NonNull String sqlContent) {
        this.jobName = jobName;
        this.jobKind = jobKind;
        this.sqlContent = sqlContent;
    }
}
