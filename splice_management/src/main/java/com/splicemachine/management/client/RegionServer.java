package com.splicemachine.management.client;

import java.io.Serializable;

public class RegionServer implements Serializable {
	String serverName;

	public RegionServer() {
		
	}
	
	public RegionServer(String serverName) {
		this.serverName = serverName;
	}
	
	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}
	
	
	
}
