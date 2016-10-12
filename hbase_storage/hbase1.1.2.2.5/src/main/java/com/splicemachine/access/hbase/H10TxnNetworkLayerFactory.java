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

package com.splicemachine.access.hbase;

import com.splicemachine.access.HConfiguration;
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
    private TableName txnTable;
    private Connection connection;

    public H10TxnNetworkLayerFactory(){ }

    @Override
    public void configure(SConfiguration config) throws IOException{
        this.connection=HBaseConnectionFactory.getInstance(config).getConnection();
        String namespace = config.getNamespace();
        String txnTable = HConfiguration.TRANSACTION_TABLE;
        this.txnTable= TableName.valueOf(namespace,txnTable);

    }

    @Override
    public TxnNetworkLayer accessTxnNetwork() throws IOException{
        Table table=connection.getTable(txnTable);
        return new H10TxnNetworkLayer(table);
    }
}
