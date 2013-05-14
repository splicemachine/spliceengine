package com.splicemachine.derby.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.distribution.CacheManagerPeerProvider;

import org.junit.Test;

public class SpliceCacheTest {

	@Test 
	public void testCache() {
		SpliceCache spliceCache1 = new SpliceCache(40001,47000, "John");
		SpliceCache spliceCache2 = new SpliceCache(40002,47001, "Jenny");
		
		waitForClusterMembership(10, TimeUnit.SECONDS, Arrays.asList("properties"), spliceCache1.getCacheManager(), spliceCache2.getCacheManager());
		spliceCache1.getCacheManager().getCache("properties").put(new Element("1","2"));
		System.out.println(spliceCache2.getCacheManager().getCache("properties").get("1"));		
		System.out.println(spliceCache1.getCacheManager().getCache("properties").get("1"));	
		
		for (int i = 0; i< 10000; i++) {
			System.out.println("--"+i);
			spliceCache1.getCacheManager().getCache("properties").put(new Element("1","2"));
			System.out.println(spliceCache2.getCacheManager().getCache("properties").get("1"));		
			System.out.println(spliceCache1.getCacheManager().getCache("properties").get("1"));	
			spliceCache1.getCacheManager().getCache("properties").remove("1");			
		}
	}
	
	   protected static void waitForClusterMembership(int time, TimeUnit unit, final Collection<String> cacheNames, final CacheManager ... managers) {
                 int peers = 0;
                 while (peers == 0) 
                 for (CacheManager manager : managers) {
                     CacheManagerPeerProvider peerProvider = manager.getCacheManagerPeerProvider("RMI");
                     for (String cacheName : cacheNames) {
                         peers = peerProvider.listRemoteCachePeers(manager.getEhcache(cacheName)).size();
                         System.out.println(peers);
                     }
                 }
     }
	
}
