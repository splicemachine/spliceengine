package com.splicemachine.constants;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Utilities for working with Configurations.
 *
 * Ensures that the splice-site.xml file is added to the Configuration
 * as a resource.
 *
 * @author Scott Fines
 * Created: 2/2/13 9:21 AM
 */
public class SpliceConfiguration {

	private static void addSpliceResources(Configuration c){
		c.addResource("splice-site.xml");
	}

	public static Configuration create(){
		Configuration conf = HBaseConfiguration.create();
		addSpliceResources(conf);
		return conf;
	}

	public static Configuration create(Configuration other){
		Configuration conf = create();
		HBaseConfiguration.merge(conf, other);
		return conf;
	}
}
