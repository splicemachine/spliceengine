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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class HConnectionPool{
    private static volatile HConnectionPool INSTANCE= new HConnectionPool(HConfiguration.getInt(HConfiguration.NUM_CONNECTIONS));
    private final Configuration[] configurations;
    private final AtomicInteger seq = new AtomicInteger(0);

    private final int size;

    public HConnectionPool(int poolSize){
        int p = 1;
        while(p<poolSize)
            p<<=1;

        this.configurations = new Configuration[p];
        for(int i=0;i<configurations.length;i++){
            this.configurations[i] = new Configuration(HConfiguration.getInstance().unwrapDelegate());
        }
        this.size = p-1;
    }

    public HConnection getConnection() throws IOException{
        return HConnectionManager.getConnection(configurations[seq.getAndIncrement() & size]);
    }

    public static HConnectionPool defaultInstance(){
        return INSTANCE;
    }
}
