package com.onework.core.job.parser;

import com.google.common.collect.Sets;
import com.onework.core.enums.JobKind;
import com.onework.core.enums.StatementKind;
import com.onework.core.enums.TemplateKind;
import com.onework.core.job.parser.statement.StatementParser;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class BaseJobParser<T> {
    private Map<StatementKind, StatementParser> statementParsers = new EnumMap<>(StatementKind.class);

    private Set<String> jobKinds = Sets.newHashSet(
            Stream.of(JobKind.values()).map(Enum::name).collect(Collectors.toList()));

    private Set<String> templateKinds = Sets.newHashSet(
            Stream.of(TemplateKind.values()).map(Enum::name).collect(Collectors.toList()));

    private Set<String> statementKinds = Sets.newHashSet(
            Stream.of(StatementKind.values()).map(Enum::name).collect(Collectors.toList()));

    public BaseJobParser() {
        bindParser(statementParsers);
    }

    protected abstract void bindParser(Map<StatementKind, StatementParser> statementParsers);

    protected abstract T onCreateJob(List<Map<String, Object>> statementsData);

    public T parse(String content) {
        checkState(StringUtils.isNotEmpty(content));

        List<String> lines = Stream.of(content.split("\n")).map(String::trim)
                .filter(StringUtils::isNotEmpty).collect(Collectors.toList());

        checkState(lines.get(0).startsWith("@"));

        List<Map<String, Object>> statementsData = new ArrayList<>();

        boolean start = false;
        boolean end = false;
        StringBuilder contentSb = new StringBuilder();
        for (String line : lines) {
            if (line.startsWith("--")) continue;
            if (start && line.endsWith(";")) {
                end = true;
                contentSb.append(line, 0, line.length() - 1);
            } else if (!start) {
                start = true;
                if (line.endsWith(";")) {
                    end = true;
                    contentSb.append(line, 0, line.length() - 1);
                } else {
                    contentSb.append(line).append(' ');
                }
            } else {
                contentSb.append(line).append(' ');
            }
            if (end) {
                content = contentSb.toString();
                StatementKind statementKind = StatementKind.SQL_STATEMENT;
                if (content.startsWith("@")) {
                    String kindString = content.substring(1, content.indexOf('{')).toUpperCase();
                    if (jobKinds.contains(kindString)) {
                        statementKind = StatementKind.JOB_ENTRY;
                    } else if (templateKinds.contains(kindString)) {
                        statementKind = StatementKind.TEMPLATE_ENTRY;
                    } else if (statementKinds.contains(kindString)) {
                        statementKind = StatementKind.valueOf(kindString);
                    }
                }
                StatementParser statementParser = statementParsers.get(statementKind);
                checkNotNull(statementParser);
                Map<String, Object> statementData = statementParser.parse(content);
                statementData.put("statementKind", statementKind);
                statementsData.add(statementData);

                contentSb.setLength(0);
                start = end = false;
            }
        }

        return onCreateJob(statementsData);
    }

    protected StatementKind getStatementKind(Map<String, Object> statementData) {
        return (StatementKind) statementData.get("statementKind");
    }
}
