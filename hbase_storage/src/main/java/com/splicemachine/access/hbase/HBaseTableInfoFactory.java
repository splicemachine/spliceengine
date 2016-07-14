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
