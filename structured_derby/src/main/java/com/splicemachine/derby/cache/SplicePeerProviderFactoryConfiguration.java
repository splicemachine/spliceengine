package com.splicemachine.derby.cache;

import net.sf.ehcache.config.FactoryConfiguration;
import net.sf.ehcache.distribution.RMICacheManagerPeerProviderFactory;

public class SplicePeerProviderFactoryConfiguration extends FactoryConfiguration {
	public SplicePeerProviderFactoryConfiguration(String multicastGroupAddress, int multicastGroupPort) {
		setClass(RMICacheManagerPeerProviderFactory.class.getCanonicalName());
		setProperties(String.format("hostName=, peerDiscovery=automatic, multicastGroupAddress=%s, multicastGroupPort=%d, timeToLive=0",multicastGroupAddress,multicastGroupPort));
	}
}
