package com.splicemachine.access.hbase;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class HConnectionPool{
    private static final HConnectionPool INSTANCE= new HConnectionPool(SpliceConstants.numHConnections);
    private final Configuration[] configurations;
    private final AtomicInteger seq = new AtomicInteger(0);

    private final int size;

    public HConnectionPool(int poolSize){
        int p = 1;
        while(p<poolSize)
            p<<=1;

        this.configurations = new Configuration[p];
        for(int i=0;i<configurations.length;i++){
            this.configurations[i] = new Configuration(SpliceConstants.config);
        }
        this.size = p-1;
    }

    public HConnection getConnection() throws IOException{
        return HConnectionManager.getConnection(configurations[seq.getAndIncrement() & size]);
    }

    public static HConnectionPool defaultInstance(){
        return INSTANCE;
    }
}
