package com.splicemachine.management.client;

import java.util.List;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("managementService")
public interface ManagementService extends RemoteService {
  List<RegionServer> getRegionServers() throws IllegalArgumentException;
}
