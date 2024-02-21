package ai.fma.impls.workloads.ldbc.snb.lightgraph;

//import java.io.BufferedInputStream;
//import java.io.BufferedOutputStream;
//import java.io.DataInputStream;
//import java.io.DataOutputStream;
import java.io.IOException;
//import java.net.Socket;
//import java.sql.Connection;
//import java.util.LinkedList;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcCallback;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.client.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.interceptor.Interceptor;
import com.baidu.brpc.protocol.Options;
import lombok.extern.slf4j.Slf4j;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.io.InputStream;

import lgraph.Lgraph;

public class LightGraphClient {

    private RpcClient client;
    private LightGraphService lgraphService;
    private String user;
    private String pass;
    private String token;

    public LightGraphClient(LightGraphClient lgraphClient) {
        this.client = lgraphClient.client;
        this.lgraphService = lgraphClient.lgraphService;
        this.token = lgraphClient.token;
        this.user = lgraphClient.user;
        this.pass = lgraphClient.pass;
    }

    public LightGraphClient(String host, int port, String user, String pass) {
        this("list://"+host+":"+port, user, pass);
    }

    public LightGraphClient(String url, String user, String pass) {
        RpcClientOptions options = new RpcClientOptions();
        options.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        options.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        options.setMaxTotalConnections(10000000);
        options.setMinIdleConnections(10);
        options.setConnectTimeoutMillis(3600000);
        options.setWriteTimeoutMillis(3600000);
        options.setReadTimeoutMillis(3600000);
        options.setTcpNoDelay(false);
        options.setChannelType(ChannelType.SINGLE_CONNECTION);
        options.setKeepAliveTime(3600);
        client = new RpcClient(url, options);
        lgraphService = BrpcProxy.getProxy(client, LightGraphService.class);

        Lgraph.LoginRequest loginReq = Lgraph.LoginRequest.newBuilder().setUser(user).setPassword(pass).build();
        Lgraph.AuthRequest authReq = Lgraph.AuthRequest.newBuilder().setLogin(loginReq).build();
        Lgraph.AclRequest req = Lgraph.AclRequest.newBuilder().setAuthRequest(authReq).build();
        Lgraph.LGraphRequest request =
                Lgraph.LGraphRequest.newBuilder().setAclRequest(req).setToken("").setIsWriteOp(false).build();

        Lgraph.LGraphResponse response = lgraphService.HandleRequest(request);
        if (response.getErrorCode().getNumber() != Lgraph.LGraphResponse.ErrorCode.SUCCESS_VALUE) {
            throw new LightGraphRpcException(response.getErrorCode(), response.getError(), "LightGraphClient");
        }

        this.token = response.getAclResponse().getAuthResponse().getToken();
        this.user = user;
        this.pass = pass;
	}

    public InputStream CallPlugin(Lgraph.PluginRequest.PluginType type, String name, String param, String graph) {
        return CallPlugin(type, name, param, graph, 0, false);
    }

    public InputStream CallPlugin(Lgraph.PluginRequest.PluginType type, String name, String param, String graph, double timeout) {
        return CallPlugin(type, name, param, graph, timeout, false);
    }

    public InputStream CallPlugin(Lgraph.PluginRequest.PluginType type, String name, String param, String graph, boolean inProcess) {
        return CallPlugin(type, name, param, graph, 0, inProcess);
    }

    public InputStream CallPlugin(Lgraph.PluginRequest.PluginType type, String name, String param, String graph, double timeout, boolean inProcess) {
        Lgraph.CallPluginRequest vreq = Lgraph.CallPluginRequest.newBuilder().setName(name).setParam(ByteString.copyFromUtf8(param)).setTimeout(timeout).setInProcess(inProcess).build();
        Lgraph.PluginRequest req = Lgraph.PluginRequest.newBuilder().setType(type).setCallPluginRequest(vreq).setGraph(graph).build();
        Lgraph.LGraphRequest request = Lgraph.LGraphRequest.newBuilder().setPluginRequest(req).setToken(this.token).build();
        Lgraph.LGraphResponse response = lgraphService.HandleRequest(request);
        if (response.getErrorCode().getNumber() != Lgraph.LGraphResponse.ErrorCode.SUCCESS_VALUE) {
            throw new LightGraphRpcException(response.getErrorCode(), response.getError(), "CallPlugin");
        }
        return response.getPluginResponse().getCallPluginResponse().getReply().newInput();
    }

	public CustomDataInputStream call(String operation, String resource, String detail) throws IOException {
	    Lgraph.PluginRequest.PluginType type = Lgraph.PluginRequest.PluginType.PYTHON;
	    if(operation.equals("CALL_CPP_PLUGIN")) {
	        type = Lgraph.PluginRequest.PluginType.CPP;
	    }
        InputStream res = CallPlugin(type, resource, detail, "default", 0, false);
	    return new CustomDataInputStream(res);
	}
    
    public Lgraph.GraphQueryResult callCypher(String query) {
        return callCypher(query, "default");
    }
    
    public Lgraph.GraphQueryResult callCypher(String query, String graph) {
        Lgraph.GraphQueryRequest queryRequest =
                Lgraph.GraphQueryRequest.newBuilder().setType(Lgraph.ProtoGraphQueryType.CYPHER).setQuery(query).setResultInJsonFormat(false)
                        .setGraph(graph).build();
        Lgraph.LGraphRequest request =
                Lgraph.LGraphRequest.newBuilder().setGraphQueryRequest(queryRequest).setToken(this.token).build();
        Lgraph.LGraphResponse response = lgraphService.HandleRequest(request);
        if (response.getErrorCode().getNumber() != Lgraph.LGraphResponse.ErrorCode.SUCCESS_VALUE) {
            throw new LightGraphRpcException(response.getErrorCode(), response.getError(), "callCypher");
        }
        //return response.getCypherResponse().getJsonResult();
        return response.getGraphQueryResponse().getBinaryResult();
    }

	public void close() throws IOException {
		client.stop();
	}

}
