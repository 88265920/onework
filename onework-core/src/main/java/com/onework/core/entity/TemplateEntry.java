package com.onework.core.entity;

import com.onework.core.converter.Map2JsonConverter;
import com.onework.core.enums.TemplateKind;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import javax.persistence.*;
import java.util.Map;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class TemplateEntry extends BaseEntity {
    @Id
    @NonNull
    private String templateName;

    @Enumerated(value = EnumType.STRING)
    @NonNull
    private TemplateKind templateKind;

    @Lob
    @Column(columnDefinition = "text")
    @Convert(converter = Map2JsonConverter.class)
    @NonNull
    private Map<String, String> templateParams;

    public TemplateEntry() {
    }

    public TemplateEntry(String templateName, @NonNull TemplateKind templateKind,
                         @NonNull Map<String, String> templateParams) {
        this.templateName = templateName;
        this.templateKind = templateKind;
        this.templateParams = templateParams;
    }
}
