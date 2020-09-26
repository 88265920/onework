package com.onework.core.job.parser.statement;

import com.onework.core.common.Constants;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class TemplateEntryParser implements StatementParser {

    @Override
    public Map<String, Object> parse(String content) {
        checkState(content.startsWith("@"));

        Map<String, Object> templateEntryData = new HashMap<>();
        int startIdx = 1;
        int endIdx = content.indexOf('{');
        Constants.TemplateKind templateKind = Constants.TemplateKind.valueOf(content.substring(startIdx, endIdx).toUpperCase());
        templateEntryData.put("templateKind", templateKind);

        startIdx = endIdx + 1;
        endIdx = content.indexOf('}');
        String paramsContent = content.substring(startIdx, endIdx);
        String[] mapStrings = paramsContent.split(",");
        Map<String, String> jobParams = new HashMap<>();
        for (String mapString : mapStrings) {
            String[] paramSp = mapString.split("=");
            jobParams.put(paramSp[0].trim(), paramSp[1]);
        }

        templateEntryData.put("templateParams", jobParams);

        return templateEntryData;
    }
}
