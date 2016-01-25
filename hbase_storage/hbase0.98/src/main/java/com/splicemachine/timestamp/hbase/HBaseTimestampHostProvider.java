package com.splicemachine.timestamp.hbase;

import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.timestamp.impl.TimestampClient;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.log4j.Logger;

/**
 * HBase-based Timestamp host provider.
 *
 * Created by jleach on 12/9/15.
 */
public class HBaseTimestampHostProvider implements TimestampHostProvider {
    private static final Logger LOG = Logger.getLogger(HBaseTimestampHostProvider.class);

    public String getHost() throws TimestampIOException{
        String hostName = null;
        try {
            HConnection connection=HConnectionManager.getConnection(HConfiguration.);
            try(HBaseAdmin admin = new HBaseAdmin(connection)){
               return admin.getClusterStatus().getMaster().getHostname();
            }
        } catch (Exception e) {
            TimestampClient.doClientErrorThrow(LOG, "Unable to determine host name for active hbase master", e);
        }
        return hostName;
    }

    public int getPort() {
        return HConfiguration.getInt(TIMESTAMP_SERVER_BIND_PORT);
    }

}
