package com.onework.core.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Data
@MappedSuperclass
@EqualsAndHashCode(callSuper = true)
public abstract class BasePKEntity extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
}
