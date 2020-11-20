package com.onework.core.job.parser.statement;

import java.util.Map;

/**
 * @author kangj
 * @date 2020/11/20
 **/
public interface StatementParser {
    Map<String, Object> parse(String content);
}
