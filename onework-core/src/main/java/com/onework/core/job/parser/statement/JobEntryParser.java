package com.onework.core.job.parser.statement;

import com.onework.core.enums.JobKind;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author kangj
 * @date 2020/11/20
 **/
public class JobEntryParser implements StatementParser {

    @Override
    public Map<String, Object> parse(String content) {
        checkState(content.startsWith("@"));

        Map<String, Object> jobEntryData = new HashMap<>();
        int startIdx = 1;
        int endIdx = content.indexOf('{');
        checkState(endIdx > 0);
        JobKind jobKind = JobKind.valueOf(content.substring(startIdx, endIdx).toUpperCase());
        jobEntryData.put("jobKind", jobKind);

        startIdx = endIdx + 1;
        endIdx = content.indexOf('}');
        checkState(endIdx > 0);
        String paramsContent = content.substring(startIdx, endIdx);
        String[] mapStrings = paramsContent.split(",");
        Map<String, String> jobArguments = new HashMap<>();
        for (String mapString : mapStrings) {
            String[] paramSp = mapString.split("=");
            checkState(paramSp.length == 2);
            jobArguments.put(paramSp[0].trim(), paramSp[1].replace("'", ""));
        }

        jobEntryData.put("jobArguments", jobArguments);

        return jobEntryData;
    }
}
