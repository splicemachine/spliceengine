package com.splicemachine.timestamp.hbase;

import com.splicemachine.timestamp.impl.TimestampClient;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import com.splicemachine.timestamp.impl.TimestampIOException;
import com.splicemachine.utils.SpliceUtilities;
import org.apache.log4j.Logger;

/**
 * Created by jleach on 12/9/15.
 */
public class HBaseTimestampHostProvider implements TimestampHostProvider {
    private static final Logger LOG = Logger.getLogger(HBaseTimestampHostProvider.class);
    public String getHost() throws TimestampIOException {
        String hostName = null;
        try {
            hostName = SpliceUtilities.getMasterServer().getHostname();
        } catch (Exception e) {
            TimestampClient.doClientErrorThrow(LOG, "Unable to determine host name for active hbase master", e);
        }
        return hostName;
    }
}
