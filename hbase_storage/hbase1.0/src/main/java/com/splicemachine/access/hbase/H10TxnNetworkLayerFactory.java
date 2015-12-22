package com.splicemachine.access.hbase;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.impl.TxnNetworkLayer;
import com.splicemachine.si.impl.TxnNetworkLayerFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class H10TxnNetworkLayerFactory implements TxnNetworkLayerFactory{
    private static final TableName TXN_TABLE=TableName.valueOf(SIConstants.spliceNamespace,SIConstants.TRANSACTION_TABLE);
    private final Connection connection;

    public H10TxnNetworkLayerFactory() throws IOException{
        this.connection=HBaseConnectionFactory.getInstance().getConnection();
    }

    @Override
    public TxnNetworkLayer accessTxnNetwork() throws IOException{
        Table table=connection.getTable(TXN_TABLE);
        return new H10TxnNetworkLayer(table);
    }
}
