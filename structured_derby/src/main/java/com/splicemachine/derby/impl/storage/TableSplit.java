package com.splicemachine.derby.impl.storage;

import com.google.common.base.Splitter;
import com.gotometrics.orderly.IntegerRowKey;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

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
        SYSCS_SPLIT_TABLE(schemaName,tableName,splitPointBuilder.toString());
    }

    public static void SYSCS_SPLIT_TABLE(String schemaName, String tableName,
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

    public static void splitTable(Connection conn,
                                       String schemaName,
                                       String tableName,
                                       String splitPoints) throws SQLException{
        long conglomId = getConglomerateId(conn, schemaName, tableName);

        HBaseAdmin admin = SpliceUtilities.getAdmin();
        doSplit(admin,splitPoints, conglomId);
//        waitForSplitsToFinish(conglomId, admin);

    }

    private static void waitForSplitsToFinish(long conglomId, HBaseAdmin admin) throws SQLException {
        boolean isSplitting = true;
        while(isSplitting){
            isSplitting=false;
            try {
                List<HRegionInfo> regions = admin.getTableRegions(Long.toString(conglomId).getBytes());
                if(regions!=null){
                    for(HRegionInfo region:regions){
                        if(region.isSplit()){
                            isSplitting=true;
                            break;
                        }
                    }
                }else{
                    isSplitting=true;
                }

                Thread.sleep(SpliceConstants.sleepSplitInterval);
            } catch (IOException e) {
                throw new SQLException(e);
            } catch (InterruptedException e) {
                throw new SQLException("Interrupted while waiting for splits to complete",e);
            }
        }
    }

    private static void doSplit(HBaseAdmin admin,String splitPoints, long conglomId) throws SQLException {
        byte[] tableName = Long.toString(conglomId).getBytes();
        RowKey rowKey = getRowKey();
        Object[] toSplit = new Object[1];
        for(String splitPosition: Splitter.on(',').trimResults().omitEmptyStrings().split(splitPoints)){
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
            try{
                int splitNum = Integer.parseInt(splitPosition);
                toSplit[0] = splitNum;
            }catch(NumberFormatException nfe){
                //not an integer, so assume you know what you're doing.
                toSplit[0] = splitPosition.getBytes();
            }
            try {
                admin.split(tableName,rowKey.serialize(toSplit));
            } catch (IOException e) {
               throw new SQLException(e.getMessage(),e);
            } catch (InterruptedException e) {
                throw new SQLException("Interrupted while attempting a split",e);
            }
            waitForSplitsToFinish(conglomId,admin);
        }
    }

    private static RowKey getRowKey() {
        //TODO -sf- improve this for the case of Primary Keys
        RowKey[] fields = new RowKey[]{new IntegerRowKey()};

        return new StructRowKey(fields);
    }

    private static long getConglomerateId(Connection conn, String schemaName, String tableName) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try{
            ps = conn.prepareStatement("select " +
                    "conglomeratenumber " +
                    "from " +
                    "sys.sysconglomerates c," +
                    "sys.systables t," +
                    "sys.sysschemas s " +
                    "where " +
                    "t.tableid = c.tableid " +
                    "and t.schemaid = s.schemaid " +
                    "and s.schemaname = ?" +
                    "and t.tablename = ?");
            ps.setString(1,schemaName.toUpperCase());
            ps.setString(2,tableName);
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getLong(1);
            }else
                throw new SQLException(String.format("No Conglomerate id found for table [%s] in schema [%s] ",tableName,schemaName.toUpperCase()));
        }finally{
            if(rs!=null) rs.close();
            if(ps!=null)ps.close();
        }
    }


    /********************************************************************************************/
    /*private helper functions*/

    private static Connection getDefaultConn() throws SQLException {
        InternalDriver id = InternalDriver.activeDriver();
        if(id!=null){
            Connection conn = id.connect("jdbc:default:connection",null);
            if(conn!=null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }

    public static void main(String... args) throws Exception{
        byte[] bytes = Bytes.toBytesBinary("Y\\x95\\x99Y\\x95\\x99");
        StructRowKey rowKey = new StructRowKey(new RowKey[]{new VariableLengthByteArrayRowKey()});
        System.out.println(Bytes.toInt((byte[])((Object[])rowKey.deserialize(bytes))[0]));
    }
}
