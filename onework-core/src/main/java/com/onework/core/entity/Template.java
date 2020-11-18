package com.onework.core.entity;

import com.onework.core.converter.MapConverter;
import com.onework.core.enums.TemplateKind;
import com.onework.core.enums.TemplateStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.*;
import java.util.Map;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class Template extends BaseEntity {
    @Id
    @NonNull
    private String templateName;

    @Enumerated(value = EnumType.STRING)
    @NonNull
    private TemplateKind templateKind;

    @Lob
    @Column(columnDefinition = "text")
    @Convert(converter = MapConverter.class)
    @NonNull
    private Map<String, String> templateArguments;

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
