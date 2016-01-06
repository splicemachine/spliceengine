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
