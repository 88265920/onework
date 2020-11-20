package com.onework.core.service;

import com.onework.core.entity.Template;
import com.onework.core.repository.TemplateRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Service
public class TemplateService {
    private final TemplateRepository templateRepository;

    @Autowired
    public TemplateService(TemplateRepository templateRepository) {
        this.templateRepository = templateRepository;
    }

    public Template findByTemplateName(String templateName) {
        return templateRepository.findById(templateName).orElse(null);
    }

    @Transactional
    public void save(Template template) {
        templateRepository.save(template);
    }

    public boolean existsByTemplateName(String templateName) {
        return templateRepository.existsById(templateName);
    }

    @Transactional
    public void deleteByTemplateName(String jobName) {
        templateRepository.deleteById(jobName);
    }
}
