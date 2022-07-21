package org.ldbcouncil.snb.impls.workload.tugraph;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.ldbcouncil.snb.driver.DbConnectionState;

public class TuGraphDbConnectionState extends DbConnectionState {
	
	private String host;
	private int port;
	private String user;
	private String pass;
	private LinkedList<TuGraphClient> clientPool;
	private TuGraphClient client;
	
	public TuGraphDbConnectionState(Map<String, String> properties) throws IOException {
		host = properties.get("host");
		port = Integer.parseInt(properties.get("port"));
		user = properties.get("user");
		pass = properties.get("pass");
		clientPool = new LinkedList<>();
		client = new TuGraphClient(host, port, user, pass);
	}
	
	public synchronized TuGraphClient popClient() throws IOException {
		if (clientPool.isEmpty()) {
			clientPool.add(new TuGraphClient(client));
		}
		return clientPool.pop();
	}
	
	public synchronized void pushClient(TuGraphClient client) {
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
