package com.onework.core.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.onework.core.conf.OneWorkConf;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.alibaba.fastjson.JSON.parseObject;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * @author kangj
 * @date 2020/11/20
 **/
@Slf4j
@Component
public class FlinkRestClient {
    private final OneWorkConf oneWorkConf;
    private final YarnClusterClient yarnClusterClient;

    @Autowired
    public FlinkRestClient(OneWorkConf oneWorkConf, YarnClusterClient yarnClusterClient) {
        this.oneWorkConf = oneWorkConf;
        this.yarnClusterClient = yarnClusterClient;
        yarnClusterClient.discover();
    }

    public JSONArray jobs() throws IOException {
        String url = String.format("http://%s:%s/jobs", yarnClusterClient.getSessionHost(),
                yarnClusterClient.getSessionPort());
        log.info("Jobs http url: {}", url);
        HttpGet get = new HttpGet(url);
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(get);
        checkState(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);
        String respData = EntityUtils.toString(response.getEntity(), UTF_8);
        return parseObject(respData).getJSONArray("jobs");
    }

    public boolean suspendJob(@NonNull String jobId) throws IOException {
        String url = String.format("http://%s:%s/proxy/%s/jobs/%s/yarn-cancel", yarnClusterClient.getRestHost(),
                yarnClusterClient.getRestPort(), yarnClusterClient.getApplicationId(), jobId);
        log.info("Suspend job http url: {}", url);
        HttpGet get = new HttpGet(url);
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(get);

        return response.getStatusLine().getStatusCode() == HttpStatus.SC_ACCEPTED;
    }

    public String suspendJobWithSavepoint(@NonNull String jobId) throws IOException, InterruptedException {
        String savepointUrl = String.format("http://%s:%s/jobs/%s/savepoints", yarnClusterClient.getSessionHost(),
                yarnClusterClient.getSessionPort(), jobId);
        log.info("Savepoint http url: {}", savepointUrl);
        HttpPost post = new HttpPost(savepointUrl);
        post.addHeader("Content-Type", "application/json");
        String postJson = String.format("{\"cancel-job\": \"true\",\"target-directory\": \"%s\"}",
                oneWorkConf.getFlink().getSavepoint());
        post.setEntity(new StringEntity(postJson));
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(post);
        checkState(response.getStatusLine().getStatusCode() == HttpStatus.SC_ACCEPTED);
        String respData = EntityUtils.toString(response.getEntity(), UTF_8);
        String requestId = parseObject(respData).getString("request-id");
        checkState(StringUtils.isNotEmpty(requestId));

        HttpGet get = new HttpGet(savepointUrl + '/' + requestId);
        log.info("RequestId http url: {}", savepointUrl + '/' + requestId);
        for (int i = 0; i < 200; i++) {
            Thread.sleep(10000);
            response = client.execute(get);
            int code = response.getStatusLine().getStatusCode();
            log.info(String.format("%d, status code = %d", i, code));
            checkState(code == HttpStatus.SC_OK);
            respData = EntityUtils.toString(response.getEntity(), UTF_8);
            log.info(respData);
            JSONObject obj = requireNonNull(parseObject(respData));
            JSONObject statusObj = requireNonNull(obj.getJSONObject("status"));
            String statusId = statusObj.getString("id");
            checkState(StringUtils.isNotEmpty(statusId));
            if (statusId.equals("COMPLETED")) {
                JSONObject operationObj = requireNonNull(obj.getJSONObject("operation"));
                return operationObj.getString("location");
            }
        }

        return null;
    }
}
