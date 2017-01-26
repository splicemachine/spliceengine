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

import org.apache.hadoop.hbase.TableName;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.STableInfoFactory;
import com.splicemachine.primitives.Bytes;

/**
 * Created by jleach on 11/18/15.
 */
public class HBaseTableInfoFactory implements STableInfoFactory<TableName> {
    private static volatile HBaseTableInfoFactory INSTANCE;
    private final String namespace;
    private final byte[] namespaceBytes;

    //use the getInstance() method instead
    private HBaseTableInfoFactory(SConfiguration config) {
        this.namespace = config.getNamespace();
        this.namespaceBytes = Bytes.toBytes(namespace);

    }

    @Override
    public TableName getTableInfo(String name) {
        return TableName.valueOf(namespace,name);
    }

    @Override
    public TableName getTableInfo(byte[] name) {
        return TableName.valueOf(namespaceBytes,name);
    }

    @Override
    public TableName parseTableInfo(String namespacePlusTable) {
        return TableName.valueOf(namespacePlusTable);
    }

    public static HBaseTableInfoFactory getInstance(SConfiguration configuration){
        HBaseTableInfoFactory htif = INSTANCE;
        if(htif==null){
            synchronized(HBaseTableInfoFactory.class){
                htif = INSTANCE;
                if(htif==null){
                    htif = INSTANCE = new HBaseTableInfoFactory(configuration);
                }
            }
        }
        return htif;
    }
}
