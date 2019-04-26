/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
