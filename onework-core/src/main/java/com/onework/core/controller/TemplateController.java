package com.onework.core.controller;

import com.google.common.io.ByteStreams;
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

@Slf4j
@RestController
@RequestMapping(path = "template")
@SuppressWarnings("rawtypes")
public class TemplateController {
    private TemplateService templateService;
    private TemplateParser templateParser;

    @Autowired
    public TemplateController(TemplateService templateService, TemplateParser templateParser) {
        this.templateService = templateService;
        this.templateParser = templateParser;
    }

    @PostMapping("create")
    @ResponseBody
    public Response create(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) return Response.error("文件不存在");

        String content;
        try {
            content = new String(ByteStreams.toByteArray(file.getInputStream()));
        } catch (IOException e) {
            log.error("", e);
            return Response.error("文件解析失败");
        }

        Template template;
        try {
            template = templateParser.parse(content);
        } catch (Exception e) {
            return Response.error(e);
        }

        if (templateService.existsByTemplateName(template.getTemplateName())) {
            return Response.error("模板已存在");
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
