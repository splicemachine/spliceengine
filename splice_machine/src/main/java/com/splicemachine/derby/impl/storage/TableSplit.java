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

package com.splicemachine.derby.impl.storage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.pipeline.ErrorState;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Splitter;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Utility to split a table.
 *
 * @author Scott Fines
 * Created on: 6/4/13
 */
public class TableSplit{
    private static final Logger LOG = Logger.getLogger(TableSplit.class);

    /**
     * Split a <em>non-primary-key</em> table into {@code numSplits} splits. Only
     * works if there is data already in the table, else HBase doesn't perform the
     * split.
     *
     * This splitting strategy relies on the assumption that values in the table
     * are uniformly distributed over the integers (e.g. no primary keys, with a prefix
     * salt as the row key). If this is not the case, this split strategy is highly unlikely
     * to be reflective of the data distribution.
     *
     * @param schemaName the name of the schema of the table
     * @param tableName the name of the table
     * @param numSplits the number of splits to break into.
     * @throws SQLException if something goes wrong.
     */
    public static void SYSCS_SPLIT_TABLE_EVENLY(String schemaName, String tableName,
                                         int numSplits) throws SQLException{
        //build split points out of integers
        if (numSplits<2) return; //no point in splitting a table that's already the right size

        long range = (long)Integer.MAX_VALUE-(long)Integer.MIN_VALUE;
        StringBuilder splitPointBuilder = new StringBuilder();
        for(int i=1;i<numSplits;i++){
            if(i!=1)
                splitPointBuilder = splitPointBuilder.append(',');
            int splitPoint = (int)(range*i/numSplits + Integer.MIN_VALUE);
            splitPointBuilder = splitPointBuilder.append(splitPoint);
        }
        SYSCS_SPLIT_TABLE_AT_POINTS(schemaName,tableName,splitPointBuilder.toString());
    }

    /**
     * Split a <em>non-primary-key</em> table. Only works if there is data already in the table, else
     * HBase doesn't perform the split.
     *
     * This splitting strategy relies on the assumption that values in the table
     * are uniformly distributed over the integers (e.g. no primary keys, with a prefix
     * salt as the row key). If this is not the case, this split strategy is highly unlikely
     * to be reflective of the data distribution.
     *
     * @param schemaName the name of the schema of the table
     * @param tableName the name of the table
     * @throws SQLException
     */
    public static void SYSCS_SPLIT_TABLE(String schemaName, String tableName) throws SQLException{
        SYSCS_SPLIT_TABLE_AT_POINTS(schemaName, tableName, null);
    }

    /**
     * Split a <em>non-primary-key</em> table into splits based on the given <code>splitPoints</code>,
     * if any. Only works if there is data already in the table, else HBase doesn't perform the
     * split.
     *
     * This splitting strategy relies on the assumption that values in the table
     * are uniformly distributed over the integers (e.g. no primary keys, with a prefix
     * salt as the row key). If this is not the case, this split strategy is highly unlikely
     * to be reflective of the data distribution.
     *
     * @param schemaName the name of the schema of the table
     * @param tableName the name of the table
     * @param splitPoints a comma-separated list of explicit table position to split on. If a split
     *                    point given, only split that particular region. If null or empty, HBase
     *                    chooses the split point.
     * @throws SQLException
     */
    public static void SYSCS_SPLIT_TABLE_AT_POINTS(String schemaName, String tableName,
                                     String splitPoints) throws SQLException{
        Connection conn = getDefaultConn();

        try{
            try{
                splitTable(conn,schemaName,tableName,splitPoints);
            }catch(SQLException se){
                try{
                    conn.rollback();
                }catch(SQLException e){
                    se.setNextException(e);
                }
                throw se;
            }
            conn.commit();
        }finally{
            try{
                if(conn!=null){
                    conn.close();
                }
            }catch(SQLException e){
                SpliceLogUtils.error(LOG,"Unable to close split connection",e);
            }
        }

    }

    /**
     * Split a region on the given <code>splictpoints</code> or, if null, let HBase determine splitpoint.
     *
     * @param regionName the name of the region
     * @param splitPoints a comma-separated list of explicit region position to split on. If a split
     *                    point given, only split that particular region If null or empty, HBase chooses
     *                    the split point.
     * @throws SQLException
     */
    public static void SYSCS_SPLIT_REGION_AT_POINTS(String regionName,
                                     String splitPoints) throws SQLException{
        if (regionName == null || regionName.isEmpty()) {
            return;
        }
        Connection conn = getDefaultConn();

        try{
            try{
                splitRegion(createRegionName(regionName),splitPoints);
            }catch(SQLException se){
                try{
                    conn.rollback();
                }catch(SQLException e){
                    se.setNextException(e);
                }
                throw se;
            }
            conn.commit();
        }finally{
            try{
                if(conn!=null){
                    conn.close();
                }
            }catch(SQLException e){
                SpliceLogUtils.error(LOG,"Unable to close split connection",e);
            }
        }

    }

    public static void splitTable(Connection conn,
                                       String schemaName,
                                       String tableName,
                                       String splitPoints) throws SQLException{
        long conglomId = getConglomerateId(conn, schemaName, tableName);

        SIDriver driver=SIDriver.driver();
        try(PartitionAdmin pa = driver.getTableFactory().getAdmin()){
            byte[][] splits = null;
            if (splitPoints != null && ! splitPoints.isEmpty()) {
                splits = getSplitPoints(splitPoints);
            }
            pa.splitTable(Long.toString(conglomId),splits);
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public static void splitRegion(byte[] regionName,
                                   String splitPoints) throws SQLException{

        SIDriver driver=SIDriver.driver();
        try(PartitionAdmin pa = driver.getTableFactory().getAdmin()){
            byte[][] splits = null;
            if (splitPoints != null && ! splitPoints.isEmpty()) {
                splits = getSplitPoints(splitPoints);
            }
            pa.splitRegion(regionName,splits);
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    /* ****************************************************************************************************************/
    /*private helper functions*/
    private static byte[][] getSplitPoints(String splitPointStrings) throws SQLException{
        Iterable<String> splits = Splitter.on(',').trimResults().omitEmptyStrings().split(splitPointStrings);
        List<byte[]> sps = new ArrayList<>(1);
        for(String split:splits){
            /*
             * If the table has no primary keys, then an easy and convenient way of splitting the
             * table is to split the salts. e.g. pick some integers between Integer.MIN_VALUE
             * and Integer.MAX_VALUE, and split around those.
             *
             * Unfortunately, serialization is generally a combination of 4 int bytes plus
             * other junk, so it's hard to know exactly whether or not this is the case.
             *
             * Because of that, if you just feed in ints, then we will parse them as ints first,
             * rather than as Strings.
             */
            byte[] pos;
            try{
                pos = Encoding.encode(Integer.parseInt(split));
            }catch(NumberFormatException nfe){
                //not an integer, so assume you know what you're doing.
                pos = Encoding.encode(Bytes.toBytes(split));
            }
            sps.add(pos);
        }
        return sps.toArray(new byte[sps.size()][]);
    }

    private static long getConglomerateId(Connection conn, String schemaName, String tableName) throws SQLException {
        try(PreparedStatement ps = conn.prepareStatement("select " +
                    "conglomeratenumber " +
                    "from " +
                    "sys.sysconglomerates c," +
                    "sys.systables t," +
                    "sys.sysschemas s " +
                    "where " +
                    "t.tableid = c.tableid " +
                    "and t.schemaid = s.schemaid " +
                    "and s.schemaname = ?" +
                    "and t.tablename = ?")){
            ps.setString(1,schemaName.toUpperCase());
            ps.setString(2,tableName);
            try(ResultSet rs = ps.executeQuery()){
                if(rs.next()){
                    return rs.getLong(1);
                }else
                    throw PublicAPI.wrapStandardException(ErrorState.TABLE_NOT_FOUND.newException(schemaName + "." + tableName));
            }
        }
    }


    private static byte[] createRegionName(String regionName) {
        return regionName.getBytes();
    }


    private static Connection getDefaultConn() throws SQLException {
        InternalDriver id = InternalDriver.activeDriver();
        if(id!=null){
            Connection conn = id.connect("jdbc:default:connection",null);
            if(conn!=null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }
}
