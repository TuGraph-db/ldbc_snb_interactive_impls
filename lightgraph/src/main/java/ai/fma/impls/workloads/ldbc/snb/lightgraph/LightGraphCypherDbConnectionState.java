package ai.fma.impls.workloads.ldbc.snb.lightgraph;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import org.ldbcouncil.snb.driver.DbConnectionState;

public class LightGraphCypherDbConnectionState extends DbConnectionState {

    private String host;
    private int rest_port;
    private int port;
    private String user;
    private String pass;
    private String updateLog;
    
    private LinkedList<LightGraphClient> clientPool;
    private LightGraphClient client;

    private static ThreadLocal<CloseableHttpClient> restClient = ThreadLocal.withInitial( () -> HttpClients.createDefault() );
    private String token;
    private String endpoint;
    
    private boolean needLogUpdate;
    private FileWriter updateLogWriter;

    public LightGraphCypherDbConnectionState(Map<String, String> properties) throws IOException {
        host = properties.get("host");
        rest_port = Integer.parseInt(properties.get("rest_port"));
        port = Integer.parseInt(properties.get("port"));
        user = properties.get("user");
        pass = properties.get("pass");
        updateLog = properties.get("update_log");
        needLogUpdate = ((updateLog!=null) && (!updateLog.equals("")));
        if (needLogUpdate)
            updateLogWriter = new FileWriter(updateLog);
        
        endpoint = host + ":" + rest_port;
        CloseableHttpClient clt = this.getClient();
        HttpPost request = new HttpPost("http://" + endpoint + "/login");
        request.setHeader("Accept", "application/json");
        String input = String.format("{\"user\":\"%s\",\"password\":\"%s\"}", user, pass);
        request.setEntity(new StringEntity(input, ContentType.APPLICATION_JSON));
        try {
            CloseableHttpResponse response = clt.execute(request);
            JSONObject jsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
            token = jsonObject.getString("jwt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        clientPool = new LinkedList<>();
        client = new LightGraphClient(host, port, user, pass);
    }
    
    public CloseableHttpClient getClient() {
        return restClient.get();
    }
    
    public String getToken() {
        return token;
    }
    
    public String getEndpoint() {
        return endpoint;
    }
    
    public void logUpdate(String log) {
        if (needLogUpdate) {
            try {
                updateLogWriter.write(log);
                updateLogWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    public synchronized LightGraphClient popClient() throws IOException {
        if (clientPool.isEmpty()) {
            clientPool.add(new LightGraphClient(client));
        }
        return clientPool.pop();
    }
    
    public synchronized void pushClient(LightGraphClient client) {
        clientPool.push(client);
    }

    @Override
    public void close() throws IOException {
        while (!clientPool.isEmpty()) {
            clientPool.pop();
//          clientPool.pop().close();
        }
        client.close();
        if (needLogUpdate)
            updateLogWriter.close();
    }

}
