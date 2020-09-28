package com.onework.core.job.parser;

import com.onework.core.entity.SqlStatement;
import com.onework.core.entity.Template;
import com.onework.core.entity.TemplateEntry;
import com.onework.core.enums.StatementKind;
import com.onework.core.enums.TemplateKind;
import com.onework.core.job.parser.statement.SqlStatementParser;
import com.onework.core.job.parser.statement.StatementParser;
import com.onework.core.job.parser.statement.TemplateEntryParser;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class TemplateParser extends BaseJobParser<Template> {

    @Override
    protected void bindParser(Map<StatementKind, StatementParser> statementParsers) {
        statementParsers.put(StatementKind.TEMPLATE_ENTRY, new TemplateEntryParser());
        statementParsers.put(StatementKind.SQL_STATEMENT, new SqlStatementParser());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Template onCreateJob(List<Map<String, Object>> statementsData) {
        Template template = new Template();
        for (Map<String, Object> statementData : statementsData) {
            StatementKind statementKind = getStatementKind(statementData);
            switch (statementKind) {
                case TEMPLATE_ENTRY:
                    Map<String, String> templateParams = (Map<String, String>) statementData.get("templateParams");
                    String templateName = templateParams.get("templateName");
                    TemplateEntry templateEntry = new TemplateEntry(templateName,
                            (TemplateKind) statementData.get("templateKind"), templateParams);
                    template.setTemplateEntry(templateEntry);
                    template.setTemplateName(templateName);
                    break;
                case SQL_STATEMENT:
                    List<SqlStatement> sqlStatements = ((List<String>) statementData.get("sqlStatements")).stream()
                            .map(s -> new SqlStatement(template.getTemplateName(), s)).collect(Collectors.toList());
                    template.setTemplateContent(sqlStatements.get(0).getSqlContent());
                    break;
                default:
                    break;
            }
        }
        return template;
    }
}
