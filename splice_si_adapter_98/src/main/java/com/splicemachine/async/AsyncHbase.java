package com.splicemachine.async;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.HConstants;

/**
 * @author Scott Fines
 *         Date: 12/15/14
 */
public class AsyncHbase {
    public static final HBaseClient HBASE_CLIENT;
    static{
        String zkQuorumStr = SpliceConstants.config.get(HConstants.ZOOKEEPER_QUORUM);
        String baseNode = SpliceConstants.config.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
        HBASE_CLIENT = new HBaseClient(zkQuorumStr,baseNode);
    }
}
