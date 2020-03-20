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

package com.splicemachine.mrio.api.core;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.client.HBaseClientSideRegionScanner;
import com.splicemachine.access.client.SkeletonClientSideRegionScanner;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mrio.MRConstants;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Ignore
public class ClientSideRegionScannerIT extends BaseMRIOTest{
    private static final Logger LOG=Logger.getLogger(ClientSideRegionScannerIT.class);
    protected static String SCHEMA_NAME=ClientSideRegionScannerIT.class.getSimpleName();
    protected static SpliceWatcher spliceClassWatcher=new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA_NAME);
    protected static SpliceTableWatcher spliceTableWatcherA=new SpliceTableWatcher("A",SCHEMA_NAME,"(col1 int, col2 varchar(56), primary key (col1))");

    protected static Connection connection;

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcherA)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try{
                        spliceClassWatcher.setAutoCommit(false);
                        PreparedStatement psA=spliceClassWatcher.prepareStatement("insert into "+SCHEMA_NAME+".A (col1,col2) values (?,?)");
                        for(int i=0;i<500;i++){
                            if(i%10!=0)
                                continue;
                            psA.setInt(1,i);
                            psA.setString(2,"dataset"+i);
                            psA.executeUpdate();
                            if(i==250)
                                flushTable(SCHEMA_NAME+".A");
                        }
                        psA=spliceClassWatcher.prepareStatement("update "+SCHEMA_NAME+".A set col2 = ? where col1 = ?");
                        PreparedStatement psB=spliceClassWatcher.prepareStatement("insert into "+SCHEMA_NAME+".A (col1,col2) values (?,?)");
                        for(int i=0;i<500;i++){
                            if(i%10==0){
                                psA.setString(1,"datasetupdate"+i);
                                psA.setInt(2,i);
                                psA.executeUpdate();
                            }else{
                                psB.setInt(1,i);
                                psB.setString(2,"dataset"+i);
                                psB.executeUpdate();
                            }
                        }
                        spliceClassWatcher.commit();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher();

    @BeforeClass
    public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(HConfiguration.unwrapDelegate());
    }

    @AfterClass
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    @Ignore
    public void validateAccurateRecordsWithStoreFileAndMemstore() throws SQLException, IOException, InterruptedException{
        int i=0;
        TableName tableName=TableName.valueOf(sqlUtil.getConglomID(SCHEMA_NAME+".A"));
        try(Admin admin = connection.getAdmin()) {
            Table table = connection.getTable(tableName);
            Scan scan=new Scan();
            scan.setCaching(50);
            scan.setBatch(50);
            scan.setMaxVersions();
            scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY,HConstants.EMPTY_BYTE_ARRAY);
            try(SkeletonClientSideRegionScanner clientSideRegionScanner=
                        new HBaseClientSideRegionScanner(table,
                              table.getConfiguration(), FSUtils.getCurrentFileSystem(table.getConfiguration()),
                              FSUtils.getRootDir(table.getConfiguration()),
                              table.getTableDescriptor(),
                              connection.getRegionLocator(tableName).getRegionLocation(scan.getStartRow()).getRegionInfo(),
                              scan,
                              connection.getRegionLocator(tableName).getRegionLocation(scan.getStartRow()).getHostnamePort())){
                List results=new ArrayList();
                while(clientSideRegionScanner.nextRaw(results)){
                    i++;
                    results.clear();
                }
            }
            Assert.assertEquals("Results Returned Are Not Accurate",500,i);
        }
    }


    @Test
    @Ignore
    public void validateAccurateRecordsWithRegionFlush() throws SQLException, IOException, InterruptedException{
        int i=0;
        TableName tableName=TableName.valueOf(sqlUtil.getConglomID(SCHEMA_NAME+".A"));
        try (Admin admin = connection.getAdmin()) {
            Table table = connection.getTable(tableName);
            Scan scan = new Scan();
            scan.setCaching(50);
            scan.setBatch(50);
            scan.setMaxVersions();
            scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY, HConstants.EMPTY_BYTE_ARRAY);

            try (SkeletonClientSideRegionScanner clientSideRegionScanner =
                       new HBaseClientSideRegionScanner(table,
                             table.getConfiguration(), FSUtils.getCurrentFileSystem(table.getConfiguration()),
                             FSUtils.getRootDir(table.getConfiguration()),
                             table.getTableDescriptor(),
                             connection.getRegionLocator(tableName).getRegionLocation(scan.getStartRow()).getRegionInfo(),
                             scan,
                             connection.getRegionLocator(tableName).getRegionLocation(scan.getStartRow()).getHostnamePort())) {
                List results = new ArrayList();
                while (clientSideRegionScanner.nextRaw(results)) {
                    i++;
                    if (i == 100)
                        admin.flush(tableName);
                    results.clear();
                }
            }
            Assert.assertEquals("Results Returned Are Not Accurate", 500, i);
        }
    }
}
