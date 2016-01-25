package com.splicemachine.access.hbase;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.si.impl.TxnNetworkLayer;
import com.splicemachine.si.impl.TxnNetworkLayerFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
@ThreadSafe
public class H10TxnNetworkLayerFactory implements TxnNetworkLayerFactory{
    private final TableName txnTable;
    private final Connection connection;

    public H10TxnNetworkLayerFactory(SConfiguration config,String namespace,String txnTableName) throws IOException{
        this.connection=HBaseConnectionFactory.getInstance(config).getConnection();
        this.txnTable= TableName.valueOf(namespace,txnTableName);
    }

    @Override
    public TxnNetworkLayer accessTxnNetwork() throws IOException{
        Table table=connection.getTable(txnTable);
        return new H10TxnNetworkLayer(table);
    }
}
