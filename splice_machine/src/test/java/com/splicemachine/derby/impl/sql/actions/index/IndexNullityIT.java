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

package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.CountGeneratedRowCreator;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
import java.sql.*;
import java.util.BitSet;

/**
 * @author Scott Fines
 *         Date: 8/28/15
 */
public class IndexNullityIT{

    private static final SpliceWatcher classWatcher=new SpliceWatcher();
    private static final SpliceSchemaWatcher schema=new SpliceSchemaWatcher(IndexNullityIT.class.getSimpleName());

    @ClassRule
    public static final TestRule chain=RuleChain.outerRule(classWatcher).around(schema);


    private final SpliceWatcher method=new SpliceWatcher(schema.schemaName);
    private TestConnection conn;

    private static final String[] indexNames=new String[]{
            //1 and 2-column indices
            "b"     , "bsi" , "bi"  , "bba" , "br"  , "bd"  , "bnu"  , "bc"  , "bv"  ,
            "sib"   , "si"  , "sii" , "siba", "sir" , "sid" , "sinu" , "sic" , "siv" ,
            "ib"    , "isi" , "i"   , "iba" , "ir"  , "id"  , "inu"  , "ic"  , "iv"  ,
            "bab"   , "basi", "bai" , "ba"  , "bar" , "bad" , "banu" , "bac" , "bav" ,
            "rb"    , "rsi" , "ri"  , "rba" , "r"   , "rd"  , "rnu"  , "rc"  , "rv"  ,
            "db"    , "dsi" , "di"  , "dba" , "dr"  , "d"   , "dnu"  , "dc"  , "dv"  ,
            "nub"   , "nusi", "nui" , "nuba", "nur" , "nud" , "nu"   , "nuc" , "nuv" ,
            "cb"    , "csi" , "ci"  , "cba" , "cr"  , "cd"  , "cnu"  , "c"   , "cv"  ,
            "vb"    , "vsi" , "vi"  , "vba" , "vr"  , "vd"  , "vnu"  , "vc"  , "v"   ,
    };

    private static final String[] indexSchemas=new String[]{
            "b"     , "b,si"    , "b,i" , "b,ba"    , "b,r" , "b,d" , "b,nu" , "b,c" , "b,v" ,
            "si,b"  , "si"      , "si,i", "si,ba"   , "si,r", "si,d", "si,nu", "si,c", "si,v",
            "i,b"   , "i,si"    , "i"   , "i,ba"    , "i,r" , "i,d" , "i,nu" , "i,c" , "i,v" ,
            "ba,b"  , "ba,si"   , "ba,i", "ba"      , "ba,r", "ba,d", "ba,nu", "ba,c", "ba,v",
            "r,b"   , "r,si"    , "r,i" , "r,ba"    , "r"   , "r,d" , "r,nu" , "r,c" , "r,v" ,
            "d,b"   , "d,si"    , "d,i" , "d,ba"    , "d,r" , "d"   , "d,nu" , "d,c" , "d,v" ,
            "nu,b"  , "nu,si"   , "nu,i", "nu,ba"   , "nu,r", "nu,d", "nu"   , "nu,c", "nu,v" ,
            "c,b"   , "c,si"    , "c,i" , "c,ba"    , "c,r" , "c,d" , "c,nu" , "c"   , "c,v" ,
            "v,b"   , "v,si"    , "v,i" , "v,ba"    , "v,r" , "v,d" , "v,nu" , "v,c" , "v"   ,
    };

    @BeforeClass
    public static void setupTables() throws Exception{
        TestConnection conn=classWatcher.getOrCreateConnection();
        conn.setSchema(schema.schemaName);
        try(Statement s = conn.createStatement()){
            s.execute("drop table if exists ALL_DTS");
        }
        TableCreator indexableDataTypes=new TableCreator(conn)
                .withCreate("create table %s "+
                        "(b boolean, si smallint, i int, ba bigint, r real, "+
                        "d double, nu numeric(10,2), c char(10),v varchar(20))");
        indexableDataTypes=indexableDataTypes.withTableName("ALL_DTS")
                .withInsert("insert into %s (b,si,i,ba,r,d,nu,c,v) values (?,?,?,?,?,?,?,?,?)")
                .withRows(new CountGeneratedRowCreator(10){
                    @Override
                    public void setRow(PreparedStatement ps) throws SQLException{
                        ps.setBoolean(1,position%2==0);
                        ps.setShort(2,(short)position);
                        ps.setInt(3,position>>>1);
                        ps.setLong(4,position>>>2);
                        ps.setFloat(5,position);
                        ps.setDouble(6,position*2d);
                        ps.setBigDecimal(7,new BigDecimal(position<<2));
                        ps.setString(8,Integer.toString(position));
                        ps.setString(9,Integer.toString(position*3));
                    }
                });
        String idxFormat="create index %s on ALL_DTS(%s)";
        //all possible combinations of 1 and 2 columns
        for(int i=0;i<indexNames.length;i++){
            indexableDataTypes = indexableDataTypes.withIndex(String.format(idxFormat,indexNames[i],indexSchemas[i]));
        }
        indexableDataTypes.create();
    }

    @Before
    public void setupTest() throws Exception{
        conn=method.getOrCreateConnection();
        conn.setSchema(schema.schemaName);
        conn.setAutoCommit(false);
    }

    @After
    public void cleanupTest() throws Exception{
        conn.rollback();
        conn.reset();
    }

    @Test
    public void testUpdateAllColumnsToNullReflectedInIndex() throws Exception{
        /*
         * The idea is to set all columns to null, then run through every index available
         * and make sure that you get null back for that row
         */
        try(Statement s=conn.createStatement()){
            s.execute("update ALL_DTS set b=null,si=null,i=null,ba=null,r=null,d=null,nu=null,c=null,v=null");

            String selectFormat = "select %s from ALL_DTS --SPLICE-PROPERTIES index=%s%n";
            BitSet nulls = new BitSet(2);
            nulls.set(1,3); //index from 1
            for(int i=0;i<indexNames.length;i++){
                String indexName=indexNames[i];
                String cols=indexSchemas[i];
                ResultSet resultSet=s.executeQuery(String.format(selectFormat,cols,indexName.toUpperCase()));
                validateQuery(indexName,cols,resultSet,nulls);
            }
        }
    }

    @Test
    public void testUpdateMiddleColToNullDoesNotCauseFollowingColsNull() throws Exception{
        String tableName = "THREE_COL_TIMESTAMP";
        String querySql = String.format("select * from %s --SPLICE-PROPERTIES index=M_COL_IDX\n" +
                "where IDX_COL=12123",tableName);
        try(Statement s = conn.createStatement()){
            s.execute("drop table if exists "+tableName);
            s.execute("create table "+tableName+"(IDX_COL INTEGER, U_COL INTEGER, CRT_DTTM TIMESTAMP)");

            s.execute("create index M_COL_IDX on "+tableName+"(IDX_COL)");

            int iCount = s.executeUpdate("insert into "+tableName+" values (12123,12149645, CURRENT_TIMESTAMP)");
            Assert.assertEquals("Incorrect number of rows updated!",1,iCount);

            Timestamp corrTimestamp;
            try(ResultSet rs = s.executeQuery(querySql)){
                Assert.assertTrue("no rows returned!",rs.next());
                int idxVal = rs.getInt(1);
                Assert.assertFalse("IDX_COL is null!",rs.wasNull());
                int uVal = rs.getInt(2);
                Assert.assertFalse("U_COL is null!",rs.wasNull());
                corrTimestamp = rs.getTimestamp(3);
                Assert.assertFalse("CRT_DTTM is null!",rs.wasNull());
                Assert.assertFalse("too many rows returned!",rs.next());
            }

            int uCount = s.executeUpdate("update "+tableName+" set U_COL=NULL where IDX_COL=12123");
            Assert.assertEquals("Incorrect number of rows updated!",1,uCount);

            try(ResultSet rs = s.executeQuery(querySql)){
                Assert.assertTrue("no rows returned!",rs.next());
                int idxVal = rs.getInt(1);
                Assert.assertFalse("IDX_COL is null!",rs.wasNull());
                int uVal = rs.getInt(2);
                Assert.assertTrue("U_COL is not null!",rs.wasNull());
                Timestamp ts = rs.getTimestamp(3);
                Assert.assertFalse("CRT_DTTM is null!",rs.wasNull());
                Assert.assertEquals("CRT_DTTM is changed by update!",corrTimestamp,ts);
                Assert.assertFalse("too many rows returned!",rs.next());
            }
        }
    }

    private void validateQuery(String indexName, String indexSchema,ResultSet resultSet,BitSet nullColumns)throws Exception{
        String errorFormat = String.format("[%s,%s]",indexName,indexSchema)+"%s";
        int colCount = resultSet.getMetaData().getColumnCount();
        while(resultSet.next()){
            for(int i=1;i<=colCount;i++){
                Object object=resultSet.getObject(i);
                if(nullColumns.get(i)){
                    //the object won't be null for primitive values, since it will cast the default primitive value for each one
//                    Assert.assertNull(String.format(errorFormat,"Object(pos="+i+",val="+object+") is unexpectedly not null!"),object);
                    Assert.assertTrue(String.format(errorFormat,"ResultSet(pos="+i+") did not think it returned null!"),resultSet.wasNull());
                }else{
                    Assert.assertNotNull(String.format(errorFormat,"Object(pos="+i+") is unexpectedly null!"),object);
                    Assert.assertFalse(String.format(errorFormat,"ResultSet(pos="+i+") thought it returned null!"),resultSet.wasNull());

                }
            }
        }
    }
}
