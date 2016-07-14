/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
/**
 * 
 * Allows access to private methods that contain scan serde
 * 
 * @author jleach
 *
 */
public class SpliceMapreduceUtils {

	  public static String convertScanToString(Scan scan) throws IOException {
		  return TableMapReduceUtil.convertScanToString(scan);
	  }
	  
	  public static Scan convertStringToScan(String base64) throws IOException {
		  return TableMapReduceUtil.convertStringToScan(base64);
	  }
}
