/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.test.framework;

import com.splicemachine.access.HConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/7/16
 */
public class RegionUtils{

    public static void splitTable(long conglomId) throws IOException, InterruptedException{
        TableName tn =TableName.valueOf("splice",Long.toString(conglomId));
        try(Connection connection = ConnectionFactory.createConnection(HConfiguration.unwrapDelegate());
            Admin admin = connection.getAdmin()) {
            int startSize = admin.getTableRegions(tn).size();
            admin.split(tn);
            while(admin.getTableRegions(tn).size()<startSize){
                Thread.sleep(200);
            }
        }
        catch (IOException e) {
            if (e.getMessage().contains("NOT splittable")) {
                Thread.sleep(200);
            }
            splitTable(conglomId);
        }
    }
}
