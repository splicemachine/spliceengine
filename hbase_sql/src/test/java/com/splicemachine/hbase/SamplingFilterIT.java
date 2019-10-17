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
 *
 */

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.region.SamplingFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class SamplingFilterIT extends SpliceUnitTest{

    private static final Logger LOG = Logger.getLogger(SamplingFilterIT.class);


    @Test
    public void testSamplingFilter() throws Exception {
        try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {

            byte[] family = Bytes.toBytes("V");
            byte[] qual = Bytes.toBytes("Q");

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("default", "filter"));
            desc.addFamily(new HColumnDescriptor(family));
            if (admin.tableExists(desc.getTableName())) {
                admin.disableTable(desc.getTableName());
                admin.truncateTable(desc.getTableName(), true);
            } else {
                admin.createTable(desc);
            }


            try (org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(HConfiguration.unwrapDelegate())) {

                Table table = connection.getTable(desc.getTableName());

                Scan s = new Scan();
                try (ResultScanner scanner = table.getScanner(s)) {
                    int count = 0;
                    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                        count++;
                    }
                    Assert.assertEquals(0, count); // table is empty
                }

                for (int i = 0; i < 100; ++i) {
                    Put p = new Put(Bytes.toBytes(i));
                    p.addColumn(family, qual, Bytes.toBytes(i));
                    table.put(p);
                }

                s.setFilter(new SamplingFilter(0.3));
                Set<Integer> distinct = new HashSet<>();
                for (int i = 0; i < 100; ++i) {
                    try (ResultScanner scanner = table.getScanner(s)) {
                        int count = 0;
                        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                            count++;
                            distinct.add(Bytes.toInt(rr.value()));
                        }
                        LOG.debug("Count " + count);
                        Assert.assertTrue(count < 55);
                        Assert.assertTrue(count > 5);
                    }
                }

                Assert.assertTrue(distinct.size() > 90);
                LOG.debug("Distinct " + distinct);
                LOG.debug("Size " + distinct.size());
            }

        }
    }


}
