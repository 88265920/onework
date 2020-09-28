package com.onework.core.entity;

import com.onework.core.enums.TemplateStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.*;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class Template extends BaseEntity {
    @Id
    @NonNull
    private String templateName;

    @OneToOne(cascade = {CascadeType.ALL}, fetch = FetchType.EAGER, orphanRemoval = true)
    @JoinColumn(name = "templateName")
    @NonNull
    private TemplateEntry templateEntry;

    @Enumerated(value = EnumType.STRING)
    @NonNull
    private TemplateStatus templateStatus;

    @Lob
    @Column(columnDefinition = "text")
    @NonNull
    private String templateContent;

    public Template() {
        // 注解生成了构造方法，无参构造方法需要创建
    }
}
