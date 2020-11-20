package com.onework.core.job.parser.statement;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author kangj
 * @date 2020/11/20
 **/
public class DependentSqlParser implements StatementParser {

    @Override
    public Map<String, Object> parse(String content) {
        checkState(content.startsWith("@"));

        Map<String, Object> dependentData = new HashMap<>();
        int startIdx = 1;
        int endIdx = content.indexOf('{');
        String dependentKind = content.substring(startIdx, endIdx).toUpperCase();

        dependentData.put("dependentKind", dependentKind);

        startIdx = endIdx + 1;
        endIdx = content.indexOf('}');
        String paramsContent = content.substring(startIdx, endIdx);
        String[] mapStrings = paramsContent.split(",");
        Map<String, String> dependentParams = new HashMap<>();
        for (String mapString : mapStrings) {
            String[] paramSp = mapString.split("=");
            dependentParams.put(paramSp[0], paramSp[1]);
        }

        dependentData.put("dependentParams", dependentParams);

        return dependentData;
    }
}
