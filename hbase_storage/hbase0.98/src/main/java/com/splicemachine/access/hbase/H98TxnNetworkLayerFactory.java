package com.splicemachine.access.hbase;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.impl.TxnNetworkLayer;
import com.splicemachine.si.impl.TxnNetworkLayerFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
@ThreadSafe
public class H98TxnNetworkLayerFactory implements TxnNetworkLayerFactory{
    private static final TableName TXN_TABLE=TableName.valueOf(SIConstants.spliceNamespace,SIConstants.TRANSACTION_TABLE);
    private final HConnectionPool connectionPool;

    public H98TxnNetworkLayerFactory(){
        this.connectionPool=HConnectionPool.defaultInstance();
    }

    @Override
    public TxnNetworkLayer accessTxnNetwork() throws IOException{
        HConnection conn = connectionPool.getConnection();
        HTableInterface table=new HTable(TXN_TABLE,conn);
        return new Hbase98TxnNetworkLayer(table);
    }
}
