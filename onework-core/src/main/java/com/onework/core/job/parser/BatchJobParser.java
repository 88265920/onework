package com.onework.core.job.parser;

import com.onework.core.common.Constants;
import com.onework.core.entity.BatchJob;
import com.onework.core.entity.JobEntry;
import com.onework.core.entity.SqlStatement;
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

@Component
public class BatchJobParser extends BaseJobParser<BatchJob> {

    private TemplateService templateService;

    public BatchJobParser(TemplateService templateService) {
        this.templateService = templateService;
    }

    @Override
    protected void bindParser(Map<Constants.StatementKind, StatementParser> statementParsers) {
        statementParsers.put(Constants.StatementKind.JOB_ENTRY, new JobEntryParser());
        statementParsers.put(Constants.StatementKind.DEPENDENT_SQL, new DependentSqlParser());
        statementParsers.put(Constants.StatementKind.SQL_STATEMENT, new SqlStatementParser());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected BatchJob onCreateJob(List<Map<String, Object>> statementsData) {
        BatchJob batchJob = new BatchJob();
        List<String> dependentJobNames = new ArrayList<>();
        List<SqlStatement> jobSqlStatements = new ArrayList<>();
        for (Map<String, Object> statementData : statementsData) {
            Constants.StatementKind statementKind = getStatementKind(statementData);
            switch (statementKind) {
                case JOB_ENTRY:
                    Map<String, String> jobParams = (Map<String, String>) statementData.get("jobParams");
                    String jobName = jobParams.get("jobName");
                    JobEntry jobEntry = new JobEntry(jobName, (Constants.JobKind) statementData.get("jobKind"), jobParams);
                    batchJob.setJobName(jobName);
                    batchJob.setJobEntry(jobEntry);
                    break;
                case DEPENDENT_SQL:
                    Map<String, String> dependentParams = (Map<String, String>) statementData.get("dependentParams");
                    dependentJobNames.add(dependentParams.get("jobName"));
                    break;
                case SQL_STATEMENT:
                    List<SqlStatement> sqlStatements = ((List<String>) statementData.get("sqlStatements")).stream()
                            .map(s -> new SqlStatement(batchJob.getJobName(), s)).collect(Collectors.toList());
                    templateService.templateReplace(sqlStatements);
                    jobSqlStatements.addAll(sqlStatements);
                    break;
                default:
                    break;
            }
        }
        batchJob.setDependentJobNames(dependentJobNames);
        batchJob.setSqlStatements(jobSqlStatements);
        return batchJob;
    }
}
