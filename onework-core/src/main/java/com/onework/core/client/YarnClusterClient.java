package com.onework.core.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.onework.core.conf.OneWorkConf;
import com.onework.core.utils.HadoopXmlUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.dom4j.DocumentException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.validation.constraints.Size;
import java.io.IOException;
import java.util.Map;

import static com.alibaba.fastjson.JSON.parseObject;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
@Component
public class YarnClusterClient {
    private static final String YARN_WEB_ADDR = "yarn.resourcemanager.webapp.address";

    private OneWorkConf oneWorkConf;
    private Map<String, String> yarnConfigs;

    private String applicationId;
    private String restHost;
    private int restPort;
    private String sessionHost;
    private int sessionPort;

    @Autowired
    public YarnClusterClient(OneWorkConf oneWorkConf) {
        this.oneWorkConf = oneWorkConf;
    }

    public void discover() {
        try {
            yarnConfigs = HadoopXmlUtils.readXML(oneWorkConf.getYarn().getSiteFile());
            yarnWebAddressDiscover();

            JSONObject appObj = yarnApplicationDetails(oneWorkConf.getYarn().getQueue(),
                    oneWorkConf.getYarn().getSessionAppName());
            if (appObj != null) {
                applicationId = appObj.getString("id");
                yarnSessionWebEndpoint(appObj.getString("amContainerLogs"));
            }
        } catch (DocumentException | IOException e) {
            log.error("", e);
        }
    }

    private void yarnWebAddressDiscover() throws IOException {
        String yarnWebAddr = yarnConfigs.get(YARN_WEB_ADDR);
        @Size(min = 2)
        String[] yarnWebAddrSp = yarnWebAddr.split(":");
        String yarnWebHost = yarnWebAddrSp[0];
        String yarnWebPort = yarnWebAddrSp[1];
        HttpGet get = new HttpGet(String.format("http://%s:%s/ws/v1/cluster", yarnWebHost,
                Integer.parseInt(yarnWebPort)));
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(get);
        checkState(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);
        restHost = yarnWebHost;
        restPort = Integer.parseInt(yarnWebPort);
    }

    private JSONObject yarnApplicationDetails(@NonNull String queue, @NonNull String appNameKey)
            throws IOException {
        HttpGet get = new HttpGet(String.format("http://%s:%s/ws/v1/cluster/apps?queue=%s",
                restHost, restPort, queue));
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(get);
        checkState(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);

        String respData = EntityUtils.toString(response.getEntity(), UTF_8);
        JSONArray array = parseObject(respData).getJSONObject("apps").getJSONArray("app");
        for (int i = 0; i < array.size(); i++) {
            JSONObject obj = array.getJSONObject(i);
            String appName = obj.getString("name");
            if (appName.contains(appNameKey)) {
                return obj;
            }
        }
        return null;
    }

    private String yarnApplicationState(@NonNull String applicationId) throws IOException {
        HttpGet get = new HttpGet(String.format("http://%s:%s/ws/v1/cluster/apps/%s/state",
                restHost, restPort, applicationId));
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(get);
        checkState(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);
        String respData = EntityUtils.toString(response.getEntity(), UTF_8);
        return parseObject(respData).getString("state");
    }

    private void yarnSessionWebEndpoint(@NonNull String containerLogPath) throws IOException {
        HttpGet get = new HttpGet(String.format("%s/jobmanager.log/?start=0&end=61440", containerLogPath));
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(get);
        checkState(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);
        String[] lines = EntityUtils.toString(response.getEntity(), UTF_8).split("\n");
        for (String line : lines) {
            if (line.contains("Web frontend listening at")) {
                String sessionEndpoint = line.substring(line.indexOf("http://") + 7, line.length() - 1);
                String[] endpoint = sessionEndpoint.split(":");
                sessionHost = endpoint[0];
                sessionPort = Integer.parseInt(endpoint[1]);
            }
        }
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getSessionHost() {
        return sessionHost;
    }

    public int getSessionPort() {
        return sessionPort;
    }

    public String getRestHost() {
        return restHost;
    }

    public int getRestPort() {
        return restPort;
    }
}
