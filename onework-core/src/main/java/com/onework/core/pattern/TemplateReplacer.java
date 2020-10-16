package com.onework.core.pattern;

import com.onework.core.ApplicationContextGetter;
import com.onework.core.entity.Template;
import com.onework.core.service.TemplateService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.onework.core.common.JobErrorMsg.TEMPLATE_NOT_EXIST;

public class TemplateReplacer extends PatternReplacer {
    private TemplateService templateService;

    @Override
    protected String tagPattern() {
        return "t|T";
    }

    @Override
    protected String afterReplace(String name, String[] argv) {
        Template template = getTemplateService().findByTemplateName(name);
        checkNotNull(template, String.format(TEMPLATE_NOT_EXIST, name));
        return template.getTemplateContent();
    }

    private TemplateService getTemplateService() {
        if (templateService == null) {
            templateService = ApplicationContextGetter.getContext().getBean(TemplateService.class);
        }

        return templateService;
    }
}
