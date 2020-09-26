package com.onework.core.job.parser.statement;

import java.util.Map;

public interface StatementParser {
    Map<String, Object> parse(String content);
}
