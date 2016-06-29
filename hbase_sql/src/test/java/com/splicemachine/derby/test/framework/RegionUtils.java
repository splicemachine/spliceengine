package com.splicemachine.derby.test.framework;

import com.splicemachine.access.HConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/7/16
 */
public class RegionUtils{

    public static void splitTable(long conglomId) throws IOException, InterruptedException{
        TableName tn =TableName.valueOf("splice",Long.toString(conglomId));
        try(HBaseAdmin admin = new HBaseAdmin(HConfiguration.unwrapDelegate())){
            int startSize = admin.getTableRegions(tn).size();
            admin.split(Long.toString(conglomId));
            while(admin.getTableRegions(tn).size()<startSize){
                Thread.sleep(200);
            }
        }
    }
}
