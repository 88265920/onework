package com.onework.core.entity;

import lombok.Data;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.GenerationTime;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.sql.Timestamp;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Data
@MappedSuperclass
public abstract class BaseEntity {
    @Column(columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", insertable = false, updatable = false)
    @Generated(GenerationTime.INSERT)
    private Timestamp createTime;

    @Column(columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", insertable = false, updatable = false)
    @Generated(GenerationTime.INSERT)
    private Timestamp updateTime;
}
