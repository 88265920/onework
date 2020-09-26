package com.onework.core.job.parser.statement;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlStatementParser implements StatementParser {

    @Override
    public Map<String, Object> parse(String content) {
        Map<String, Object> dependentData = new HashMap<>();
        dependentData.put("sqlStatements", Stream.of(content.split(";")).collect(Collectors.toList()));
        return dependentData;
    }
}
