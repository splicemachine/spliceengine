package com.splicemachine.derby.cache;

import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Searchable;
import net.sf.ehcache.distribution.MulticastKeepaliveHeartbeatSender;
import net.sf.ehcache.distribution.RMICacheReplicatorFactory;

public class SpliceCacheConfiguration extends CacheConfiguration {
	public SpliceCacheConfiguration(String name, int maxEntries) {
		super();
		MulticastKeepaliveHeartbeatSender.setHeartBeatInterval(1000);
		setTimeToIdleSeconds(100);
		setTimeToLiveSeconds(100);
		setName(name);
		setMaxEntriesLocalHeap(maxEntries);
		Searchable searchable = new Searchable();
		searchable.setKeys(true);
		searchable.setValues(true);
		searchable.setAllowDynamicIndexing(true);
		searchable(searchable);
		CacheEventListenerFactoryConfiguration cacheEventListenerFactoryConfiguration = new CacheEventListenerFactoryConfiguration();
		cacheEventListenerFactoryConfiguration.setClass(RMICacheReplicatorFactory.class.getCanonicalName());
		cacheEventListenerFactoryConfiguration.setProperties("replicateAsynchronously=false, replicatePuts=false, replicateUpdates=false, replicateUpdatesViaCopy=false, replicateRemovals=true ");
		addCacheEventListenerFactory(cacheEventListenerFactoryConfiguration);
	}
}
