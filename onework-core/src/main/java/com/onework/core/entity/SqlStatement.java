package com.onework.core.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class SqlStatement extends BasePKEntity {

    @NonNull
    private String jobName;

    @Lob
    @Column(columnDefinition = "text")
    @NonNull
    private String sqlContent;

    public SqlStatement() {
    }

    public SqlStatement(@NonNull String jobName, @NonNull String sqlContent) {
        this.jobName = jobName;
        this.sqlContent = sqlContent;
    }
}
