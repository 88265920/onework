package com.onework.core.job.parser;

import com.onework.core.entity.SqlStatement;
import com.onework.core.entity.StreamJob;
import com.onework.core.enums.StatementKind;
import com.onework.core.job.parser.statement.DependentSqlParser;
import com.onework.core.job.parser.statement.JobEntryParser;
import com.onework.core.job.parser.statement.SqlStatementParser;
import com.onework.core.job.parser.statement.StatementParser;
import com.onework.core.service.TemplateService;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@Component
public class StreamJobParser extends BaseJobParser<StreamJob> {
    private TemplateService templateService;

    public StreamJobParser(TemplateService templateService) {
        this.templateService = templateService;
    }

    @Override
    protected void bindParser(Map<StatementKind, StatementParser> statementParsers) {
        statementParsers.put(StatementKind.JOB_ENTRY, new JobEntryParser());
        statementParsers.put(StatementKind.DEPENDENT_SQL, new DependentSqlParser());
        statementParsers.put(StatementKind.SQL_STATEMENT, new SqlStatementParser());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StreamJob onCreateJob(List<Map<String, Object>> statementsData) {
        StreamJob streamJob = new StreamJob();
        List<String> dependentJobNames = new ArrayList<>();
        List<SqlStatement> jobSqlStatements = new ArrayList<>();
        for (Map<String, Object> statementData : statementsData) {
            StatementKind statementKind = getStatementKind(statementData);
            switch (statementKind) {
                case JOB_ENTRY:
                    parseJobEntry(statementData, streamJob);
                    break;
                case DEPENDENT_SQL:
                    parseDependentJob(statementData, dependentJobNames);
                    break;
                case SQL_STATEMENT:
                    checkArgument(statementData.containsKey("sqlStatements"));
                    List<SqlStatement> sqlStatements = ((List<String>) statementData.get("sqlStatements")).stream()
                            .map(s -> new SqlStatement(streamJob.getJobName(), s)).collect(Collectors.toList());
                    templateService.templateReplace(sqlStatements);
                    jobSqlStatements.addAll(sqlStatements);
                    break;
                default:
                    break;
            }
        }
        streamJob.setDependentJobNames(dependentJobNames);
        streamJob.setSqlStatements(jobSqlStatements);
        return streamJob;
    }
}
