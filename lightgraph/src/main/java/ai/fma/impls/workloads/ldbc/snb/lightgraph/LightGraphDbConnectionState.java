package ai.fma.impls.workloads.ldbc.snb.lightgraph;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.ldbcouncil.snb.driver.DbConnectionState;

public class LightGraphDbConnectionState extends DbConnectionState {
	
	private String host;
	private int port;
	private String user;
	private String pass;
	private LinkedList<LightGraphClient> clientPool;
	private LightGraphClient client;
	
	public LightGraphDbConnectionState(Map<String, String> properties) throws IOException {
		host = properties.get("host");
		port = Integer.parseInt(properties.get("port"));
		user = properties.get("user");
		pass = properties.get("pass");
		clientPool = new LinkedList<>();
		client = new LightGraphClient(host, port, user, pass);
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
//			clientPool.pop().close();
		}
		client.close();
	}

}
