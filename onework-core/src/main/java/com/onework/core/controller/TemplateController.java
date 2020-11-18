package com.onework.core.controller;

import com.google.common.io.ByteStreams;
import com.onework.core.common.Response;
import com.onework.core.entity.Template;
import com.onework.core.enums.TemplateStatus;
import com.onework.core.job.parser.TemplateParser;
import com.onework.core.service.TemplateService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

import static com.onework.core.common.JobErrorMsg.*;

@Slf4j
@RestController
@RequestMapping(path = "template")
@SuppressWarnings("rawtypes")
public class TemplateController {
    private final TemplateService templateService;
    private final TemplateParser templateParser;

    @Autowired
    public TemplateController(TemplateService templateService, TemplateParser templateParser) {
        this.templateService = templateService;
        this.templateParser = templateParser;
    }

    @PostMapping("create")
    @ResponseBody
    public Response create(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) return Response.error(FILE_NOT_EXIST_OR_EMPTY);

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            log.error("", e);
            return Response.error(FILE_PARSING_FAILED);
        }

        Template template;
        try {
            template = templateParser.parse(content);
        } catch (Exception e) {
            return Response.error(e);
        }

        if (templateService.existsByTemplateName(template.getTemplateName())) {
            return Response.error(TEMPLATE_EXISTED);
        } else {
            template.setTemplateStatus(TemplateStatus.CREATED);
            templateService.save(template);
        }

        return Response.ok();
    }

    @GetMapping("delete")
    @ResponseBody
    @Transactional
    public Response delete(@NonNull String templateName) {
        if (!templateService.existsByTemplateName(templateName)) return Response.error("模板不存在");
        templateService.deleteByTemplateName(templateName);
        return Response.ok();
    }
}
