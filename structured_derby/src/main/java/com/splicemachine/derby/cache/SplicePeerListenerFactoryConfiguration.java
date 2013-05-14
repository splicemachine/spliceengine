package com.splicemachine.derby.cache;

import net.sf.ehcache.config.FactoryConfiguration;
import net.sf.ehcache.distribution.RMICacheManagerPeerListenerFactory;

public class SplicePeerListenerFactoryConfiguration extends FactoryConfiguration {
	public SplicePeerListenerFactoryConfiguration(int port, int remoteObjectPort) {
		setClass(RMICacheManagerPeerListenerFactory.class.getCanonicalName());
		setProperties(String.format("hostName=, port=%d, remoteObjectPort=%d, socketTimeoutMillis=",port,remoteObjectPort));
	}
}
