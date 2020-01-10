/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
