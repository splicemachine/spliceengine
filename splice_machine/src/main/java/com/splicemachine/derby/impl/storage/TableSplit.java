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

package com.splicemachine.derby.impl.storage;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Splitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility to split a table.
 *
 * @author Scott Fines
 * Created on: 6/4/13
 */
public class TableSplit{
    private static final Logger LOG = Logger.getLogger(TableSplit.class);

    public static void SYSCS_SPLIT_TABLE_OR_INDEX(String schemaName,
                                                  String tableName,
                                                  String indexName,
                                                  String insertColumnList,
                                                  String fileName,
                                                  String columnDelimiter,
                                                  String characterDelimiter,
                                                  String timestampFormat,
                                                  String dateFormat,
                                                  String timeFormat,
                                                  long badRecordsAllowed,
                                                  String badRecordDirectory,
                                                  String oneLineRecords,
                                                  String charset,
                                                  ResultSet[] results
    ) throws Exception {
        EmbedConnection defaultConn=(EmbedConnection) SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        LanguageConnectionContext lcc = lastActivation.getLanguageConnectionContext();

        if (tableName == null) {
            throw StandardException.newException(SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }

        if (schemaName == null) {
            schemaName = lcc.getCurrentSchemaName();
        }

        long conglomId = getConglomerateId(defaultConn, schemaName, tableName, indexName);
        String tempDir = new Path(badRecordDirectory, ".TMP").toString();
        String sql = "call syscs_util.compute_split_key(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = defaultConn.prepareStatement(sql);
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, indexName);
        ps.setString(4, insertColumnList);
        ps.setString(5, fileName);
        ps.setString(6, columnDelimiter);
        ps.setString(7, characterDelimiter);
        ps.setString(8, timestampFormat);
        ps.setString(9, dateFormat);
        ps.setString(10, timeFormat);
        ps.setLong(11, badRecordsAllowed);
        ps.setString(12, badRecordDirectory);
        ps.setString(13, oneLineRecords);
        ps.setString(14, charset);
        ps.setString(15, tempDir);
        ps.executeQuery();
        ps.close();

        DistributedFileSystem fs = SIDriver.driver().getFileSystem(tempDir);
        Path filePath = new Path(tempDir, conglomId + "/keys");
        InputStream in = fs.newInputStream(filePath.toString(), StandardOpenOption.READ);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String splitKey = null;
        while((splitKey = br.readLine()) != null) {
            splitTable(schemaName, tableName, indexName, splitKey);
        }
    }
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
                                         int numSplits) throws SQLException, StandardException {
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
    public static void SYSCS_SPLIT_TABLE(String schemaName, String tableName) throws SQLException, StandardException {
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
                                     String splitPoints) throws SQLException, StandardException {
        splitTable(schemaName, tableName, null, splitPoints);
    }

    public static void SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS(String schemaName, String tableName,
                                                   String indexName, String splitPoints) throws SQLException, StandardException {
        splitTable(schemaName, tableName, indexName, splitPoints);
    }

    private static void splitTable(String schemaName, String tableName,
                                   String indexName, String splitPoints) throws SQLException, StandardException {
        Connection conn = getDefaultConn();

        try{
            try{
                splitTable(conn,schemaName,tableName,indexName,splitPoints);
            }catch(SQLException se){
                try{
                    conn.rollback();
                }catch(SQLException e){
                    se.setNextException(e);
                }
                throw se;
            } catch (StandardException e) {
                throw e;
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

    public static void splitTable(Connection conn, String schemaName, String tableName,
                                  String indexName, String splitPoints) throws SQLException, StandardException {

        EmbedConnection defaultConn=(EmbedConnection) SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        LanguageConnectionContext lcc = lastActivation.getLanguageConnectionContext();

        if (tableName == null) {
            throw StandardException.newException(SQLState.TABLE_NAME_CANNOT_BE_NULL);
        }

        if (schemaName == null) {
            schemaName = lcc.getCurrentSchemaName();
        }

        long conglomId = getConglomerateId(conn, schemaName, tableName, indexName);

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
            byte[] pos = Bytes.toBytesBinary(split);

            sps.add(pos);
        }
        return sps.toArray(new byte[sps.size()][]);
    }

    public static long getConglomerateId(Connection conn, String schemaName, String tableName, String indexName) throws SQLException, StandardException {

        String sql =  "select " +
                "conglomeratenumber " +
                "from " +
                "sysvw.sysconglomerateinschemas c " +
                "where " +
                "schemaname = ? " +
                "and tablename = ? ";

        if (indexName != null)
            sql += "and conglomeratename = ?";
        sql += " order by 1";
        
        try(PreparedStatement ps = conn.prepareStatement(sql)){
            ps.setString(1,schemaName.toUpperCase());
            ps.setString(2,tableName);
            if (indexName != null)
                ps.setString(3, indexName);
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
