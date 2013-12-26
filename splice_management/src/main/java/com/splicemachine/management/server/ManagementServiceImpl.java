package com.splicemachine.management.server;

import java.util.Arrays;
import java.util.List;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.splicemachine.management.client.ManagementService;
import com.splicemachine.management.client.RegionServer;

public class ManagementServiceImpl extends RemoteServiceServlet implements ManagementService {

	public List<RegionServer> getRegionServers() throws IllegalArgumentException {
		return Arrays.asList(new RegionServer("john"),new RegionServer("perry"));
	}

	
}
