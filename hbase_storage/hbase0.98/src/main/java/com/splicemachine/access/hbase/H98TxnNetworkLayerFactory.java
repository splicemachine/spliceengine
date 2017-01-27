/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
    private static final TableName TXN_TABLE=TableName.valueOf(namespace,txnTable);
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
