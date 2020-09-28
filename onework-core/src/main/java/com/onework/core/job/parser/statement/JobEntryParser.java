package com.onework.core.job.parser.statement;

import com.onework.core.enums.JobKind;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class JobEntryParser implements StatementParser {

    @Override
    public Map<String, Object> parse(String content) {
        checkState(content.startsWith("@"));

        Map<String, Object> jobEntryData = new HashMap<>();
        int startIdx = 1;
        int endIdx = content.indexOf('{');
        JobKind jobKind = JobKind.valueOf(content.substring(startIdx, endIdx).toUpperCase());
        jobEntryData.put("jobKind", jobKind);

        startIdx = endIdx + 1;
        endIdx = content.indexOf('}');
        String paramsContent = content.substring(startIdx, endIdx);
        String[] mapStrings = paramsContent.split(",");
        Map<String, String> jobParams = new HashMap<>();
        for (String mapString : mapStrings) {
            String[] paramSp = mapString.split("=");
            jobParams.put(paramSp[0].trim(), paramSp[1]);
        }

        jobEntryData.put("jobParams", jobParams);

        return jobEntryData;
    }
}
