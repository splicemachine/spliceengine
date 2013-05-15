package com.splicemachine.derby.cache;

import com.splicemachine.constants.SpliceConstants;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.Configuration;

/**
 * 
 * Begin a EHCache with the following configuration
 * 
 *  <cacheManagerPeerProviderFactory class="net.sf.ehcache.distribution.RMICacheManagerPeerProviderFactory"
 *                                    properties="hostName=, peerDiscovery=automatic, multicastGroupAddress=230.0.0.1,
 *                                     multicastGroupPort=4446, timeToLive=0"/>
 * 
 * 
 * <cacheManagerPeerListenerFactory class="net.sf.ehcache.distribution.RMICacheManagerPeerListenerFactory"
 *                                    properties="hostName=, port=40001, remoteObjectPort=47000, socketTimeoutMillis="/>
 *        <defaultCache
 *           maxElementsInMemory="10"
 *           eternal="false"
 *           timeToIdleSeconds="100"
 *           timeToLiveSeconds="100"
 *           overflowToDisk="true">
 *       <cacheEventListenerFactory class="net.sf.ehcache.distribution.RMICacheReplicatorFactory"
 *                                  properties="replicateAsynchronously=true,
 *                                              replicatePuts=true,
 *                                              replicateUpdates=true,
 *                                              replicateUpdatesViaCopy=true,
 *                                              replicateRemovals=true "/>
 *       <bootstrapCacheLoaderFactory class="net.sf.ehcache.distribution.RMIBootstrapCacheLoaderFactory" />
 *   </defaultCache>
 *                                    
 *                                    
 *
 * 
 * @author johnleach
 *
 */
public class SpliceCache {
	protected CacheManager cacheManager;	
	
	public SpliceCache(String name) {
		this(SpliceConstants.rmiPort,SpliceConstants.rmiRemoteObjectPort,name);
	}
	
	public SpliceCache(int port, int objectPort, String name) {
		Configuration config = new Configuration();
		config.setName(name);
		config.addCacheManagerPeerProviderFactory(new SplicePeerProviderFactoryConfiguration(SpliceConstants.multicastGroupAddress,SpliceConstants.multicastGroupPort));
		config.addCacheManagerPeerListenerFactory(new SplicePeerListenerFactoryConfiguration(port,objectPort));
		cacheManager = new CacheManager(config);
		Cache cache = new Cache(new SpliceCacheConfiguration("properties", 1000));		
		cacheManager.addCache(cache);		
	}


	public CacheManager getCacheManager() {
		return cacheManager;
	}


	public void setCacheManager(CacheManager cacheManager) {
		this.cacheManager = cacheManager;
	}
	
	
	
}

