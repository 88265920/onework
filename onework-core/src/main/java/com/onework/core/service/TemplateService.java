package com.onework.core.service;

import com.onework.core.entity.SqlStatement;
import com.onework.core.entity.Template;
import com.onework.core.repository.TemplateRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class TemplateService {
    private TemplateRepository templateRepository;

    @Autowired
    public TemplateService(TemplateRepository templateRepository) {
        this.templateRepository = templateRepository;
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

    public void templateReplace(List<SqlStatement> sqlStatements) {
        for (SqlStatement sqlStatement : sqlStatements) {
            String content = sqlStatement.getSqlContent();
            StringBuilder contentSb = new StringBuilder();
            int startIdx = 0;
            int endIdx;
            while ((endIdx = content.indexOf('{', startIdx)) > 0) {
                String prefixContent = content.substring(startIdx, endIdx);
                contentSb.append(prefixContent);
                startIdx = content.indexOf('}', startIdx) + 1;
                String templateName = content.substring(endIdx + 1, startIdx - 1);
                Optional<Template> template = templateRepository.findById(templateName);
                template.ifPresent(t -> contentSb.append(t.getTemplateContent()));
            }
            contentSb.append(content.substring(startIdx));
            sqlStatement.setSqlContent(contentSb.toString());
        }
    }
}
