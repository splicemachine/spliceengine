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

package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.splicemachine.derby.stream.output.WriteReadUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.impl.HOperationFactory;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.storage.DataScan;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;

public class SMSQLUtil  {
    static final Logger LOG = Logger.getLogger(SMSQLUtil.class);
    private Connection connect = null;
    private static SMSQLUtil sqlUtil = null;
    private String connStr = null;

    private SMSQLUtil(String connStr) throws Exception {
        this.connStr = connStr;
        connect = createConn();
    }

    public static SMSQLUtil getInstance(String connStr){
        if(sqlUtil == null){
            try {
                sqlUtil = new SMSQLUtil(connStr);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return sqlUtil;
    }

    public Connection createConn() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
        Class.forName(SQLConfiguration.SPLICE_JDBC_DRIVER).newInstance();
        return DriverManager.getConnection(connStr);
    }

    public void disableAutoCommit(Connection conn) throws SQLException{
        conn.setAutoCommit(false);
    }

    public void commit(Connection conn) throws SQLException{
        conn.commit();
    }

    public void rollback(Connection conn) throws SQLException{
        conn.rollback();
    }

    /**
     * Get primary key from 'tableName'
     * Return Column Name list : Column Seq list
     * Column Id here refers to Column Seq, where Id=1 means the first column in primary keys.
     * @throws SQLException
     *
     **/
    public List<PKColumnNamePosition> getPrimaryKeys(String tableName) throws SQLException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getPrimaryKeys tableName=%s", tableName);
        ArrayList<PKColumnNamePosition> pkCols = new ArrayList<PKColumnNamePosition>(1);
        String[] schemaTable = parseTableName(tableName);
        ResultSet result = null;
        try {
            DatabaseMetaData databaseMetaData = connect.getMetaData();
            result = databaseMetaData.getPrimaryKeys(null, schemaTable[0], schemaTable[1]);
            while(result.next()){
                // Convert 1-based column index to 0-based index
                pkCols.add(new PKColumnNamePosition(result.getString(4), result.getInt(5)-1));
            }
        } finally {
            if (result != null)
                result.close();
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getPrimaryKeys returns=%s", Arrays.toString(pkCols.toArray()));
        return pkCols.size()!=0?pkCols:null;
    }


    /**
     *
     * Get table structure of 'tableName'
     * Return Column Name list : Column Type list
     * @throws SQLException
     *
     * */
    public List<NameType> getTableStructure(String tableName) throws SQLException{
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getTableStructure tableName=%s", tableName);
        List<NameType> colType = new ArrayList<NameType>();
        ResultSet result = null;
        try{
            String[] schemaTableName = parseTableName(tableName);
            DatabaseMetaData databaseMetaData = connect.getMetaData();
            result = databaseMetaData.getColumns(null, schemaTableName[0],  schemaTableName[1], null);
            while(result.next()){
                colType.add(new NameType(result.getString(4), result.getInt(5)));
            }
        } finally {
            if (result != null)
                result.close();
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getTableStructure returns=%s", Arrays.toString(colType.toArray()));
        return colType;
    }

    public Connection getStaticConnection(){
        return this.connect;
    }

    /**
     *
     * Get ConglomID from 'tableName'
     * Param is Splice tableName with schema, pattern: schema.tableName
     * Return ConglomID
     * ConglomID means HBase table Name which maps to the Splice table Name
     * @throws SQLException
     *
     * */
    public String getConglomID(String tableName) throws SQLException{
        String[] schemaTableName = parseTableName(tableName);
        long[] conglomIds = SpliceAdmin.getConglomNumbers(connect, schemaTableName[0], schemaTableName[1]);
        StringBuffer str = new StringBuffer();
        str.append(conglomIds[0]);
        return str.toString();
    }

    /**
     *
     * Get TransactionID
     * Each Transaction has a uniq ID
     * For every map job, there should be a different transactionID.
     *
     * */
    public String getTransactionID() throws SQLException {
        String trxId = "";
        ResultSet resultSet = null;
        try {
            resultSet = connect.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
            while(resultSet.next()){
                trxId = String.valueOf(resultSet.getLong(1));
            }
        } finally {
            if (resultSet != null)
                resultSet.close();
        }
        return trxId;
    }

    public String getTransactionID(Connection conn) throws SQLException{
        String trxId = null;
        ResultSet resultSet = null;
        try {
            resultSet = conn.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
            while(resultSet.next()){
                long txnID = resultSet.getLong(1);
                trxId = String.valueOf(txnID);
            }
        } finally {
            if (resultSet != null)
                resultSet.close();
        }
        return trxId;
    }

    public long getChildTransactionID(Connection conn, long parentTxsID, String tableName) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        long childTxsID;
        try {
            ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_START_CHILD_TRANSACTION(?,?)");
            ps.setLong(1, parentTxsID);
            ps.setString(2, tableName);
            ResultSet rs3 = ps.executeQuery();
            rs3.next();
            childTxsID = rs3.getLong(1);
        } finally {
            if (rs != null)
                rs.close();
            if (ps != null)
                ps.close();
        }
        return childTxsID;
    }

    public void commitChildTransaction(Connection conn, long childTxnID) throws SQLException{
        PreparedStatement ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_COMMIT_CHILD_TRANSACTION(?)");
        ps.setLong(1, childTxnID);
        ps.execute();
    }


    public void closeConn(Connection conn) throws SQLException{
        conn.close();
    }

    public void closeQuietly() {
        try {
            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public boolean checkTableExists(String tableName) throws SQLException{
        ResultSet rs = null;
        try {
            DatabaseMetaData meta = connect.getMetaData();
            String[] schemaTableName = parseTableName(tableName);
            rs = meta.getTables(null, schemaTableName[0], schemaTableName[1], new String[] { "TABLE" });
            while (rs.next()) {
                return true;
            }
            return false;
        } finally {
            if (rs != null)
                rs.close();
        }
    }

    private String[] parseTableName(String str) throws SQLException{
        str = str.toUpperCase();
        if(str == null || str.trim().equals(""))
            return null;
        else{
            String[] tmp = str.split("\\.");
            if (tmp.length== 2) {
                return tmp;
            }
            if(tmp.length == 1){
                String[] s = new String[2];
                s[1] = tmp[0];
                s[0] = null;
                return s;
            }
            throw new SQLException("Splice table not known, "
                    + "please specify Splice tableName. "
                    + "pattern: schemaName.tableName");
        }
    }

    public int[] getRowDecodingMap(List<NameType> nameTypes, List<PKColumnNamePosition> primaryKeys, List<String> columnNames) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getRowDecodingMap nameTypes=%s, primaryKeys=%s, columnNames=%s",nameTypes,
                    primaryKeys,columnNames);

        int []rowDecodingMap = IntArrays.count(nameTypes.size());
        for (int i = 0; i< nameTypes.size(); i++) {
            NameType nameType = nameTypes.get(i);
            if (!isPrimaryKeyColumn(primaryKeys, nameType.getName()) && (columnNames.contains(nameType.getName())))
                rowDecodingMap[i] = columnNames.indexOf(nameType.getName());
            else
                rowDecodingMap[i] = -1;
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getRowDecodingMap returns=%s",Arrays.toString(rowDecodingMap));
        return rowDecodingMap;
    }

    private boolean isPrimaryKeyColumn(List<PKColumnNamePosition> primaryKeys, String name) {
        boolean isPkCol = false;
        if (primaryKeys != null && primaryKeys.size() > 0) {
            for (PKColumnNamePosition namePosition: primaryKeys) {
                if (namePosition.getName().compareToIgnoreCase(name) == 0) {
                    isPkCol = true;
                    break;
                }
            }
        }
        return isPkCol;
    }

    public int[] getKeyColumnEncodingOrder(List<NameType> nameTypes, List<PKColumnNamePosition> primaryKeys) {
        int[] keyColumnEncodingOrder = new int[0];
        if (primaryKeys!=null && primaryKeys.size() > 0) {
            keyColumnEncodingOrder = IntArrays.count(primaryKeys.size());
            for (int i = 0; i < primaryKeys.size(); i++) {
                keyColumnEncodingOrder[primaryKeys.get(i).getPosition()] = locationInNameTypes(nameTypes, primaryKeys.get(i).getName());
            }
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getKeyColumnEncodingOrder returns=%s", Arrays.toString(keyColumnEncodingOrder));
        return keyColumnEncodingOrder;
    }

    public int[] getKeyDecodingMap(int[] keyColumnEncodingOrder, List<String> columnNames, List<NameType> nameTypes) {
        int[] keyDecodingMap = IntArrays.count(keyColumnEncodingOrder.length);
        for (int i = 0; i< keyColumnEncodingOrder.length; i++) {
            keyDecodingMap[i] = columnNames.indexOf(nameTypes.get(keyColumnEncodingOrder[i]).name);
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getKeyDecodingMap returns=%s",Arrays.toString(keyDecodingMap));
        return keyDecodingMap;
    }

    public int[] getKeyColumnTypes(int[] keyColumnEncodingOrder, List<NameType> nameTypes) throws StandardException {
        int[] keyColumnTypes = IntArrays.count(keyColumnEncodingOrder.length);
        for (int i = 0; i< keyColumnEncodingOrder.length; i++) {
            keyColumnTypes[i] = nameTypes.get(keyColumnEncodingOrder[i]).getTypeFormatId();
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getKeyColumnTypes returns=%s",Arrays.toString(keyColumnTypes));
        return keyColumnTypes;
    }

    public int[] getExecRowFormatIds(List<String> columnNames, List<NameType> nameTypes) throws StandardException {
        int[] execRowFormatIds = new int[columnNames==null?nameTypes.size():columnNames.size()];
        if (columnNames != null) {
            for (int i = 0; i< columnNames.size(); i++) {
                execRowFormatIds[i] = nameTypes.get(locationInNameTypes(nameTypes,columnNames.get(i))).getTypeFormatId();
            }
        } else {
            for (int i = 0; i< nameTypes.size(); i++) {
                execRowFormatIds[i] = nameTypes.get(i).getTypeFormatId();
            }
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getExecRowFormatIds returns=%s",execRowFormatIds);
        return execRowFormatIds;
    }

    public static ExecRow getExecRow(int[] execRowFormatIds) throws IOException {
        ExecRow execRow = new ValueRow(execRowFormatIds==null?0:execRowFormatIds.length);
        try {
            for (int i = 0; i< execRow.nColumns(); i++) {
                execRow.setColumn(i+1, LazyDataValueFactory.getLazyNull(execRowFormatIds[i]));
            }
        } catch (StandardException se) {
            throw new IOException(se);
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getExecRow returns=%s",execRow);
        return execRow;
    }


    public FormatableBitSet getAccessedKeyColumns(int[] keyColumnEncodingOrder,int[] keyDecodingMap) {
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(keyColumnEncodingOrder.length);
        for(int i=0;i<keyColumnEncodingOrder.length;i++) {
            accessedKeyColumns.set(i);
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getAccessedKeyColumns returns=%s",accessedKeyColumns);
        return accessedKeyColumns;
    }
    private int locationInNameTypes(List<NameType> nameTypes, String name) {
        for (int i = 0; i< nameTypes.size(); i++) {
            if (nameTypes.get(i).name.equals(name))
                return i;
        }
        throw new RuntimeException("misssing element");
    }

    public ScanSetBuilder getTableScannerBuilder(String tableName, List<String> columnNames) throws SQLException {
        List<PKColumnNamePosition> primaryKeys = getPrimaryKeys(tableName);
        List<NameType> nameTypes = getTableStructure(tableName);
        if (columnNames ==null) {
            columnNames =new ArrayList<>(nameTypes.size());
            for (int i = 0; i< nameTypes.size(); i++) {
                columnNames.add(nameTypes.get(i).name);
            }
        }
        int[] rowDecodingMap = getRowDecodingMap(nameTypes,primaryKeys,columnNames);
        int[] keyColumnEncodingOrder = getKeyColumnEncodingOrder(nameTypes,primaryKeys);
        boolean[] keyColumnOrdering = new boolean[keyColumnEncodingOrder.length];
        for (int i = 0; i< keyColumnEncodingOrder.length; i++) {
            keyColumnOrdering[i] = true;
        }
        int[] keyDecodingMap = getKeyDecodingMap(keyColumnEncodingOrder,columnNames,nameTypes);
        int[] keyColumnTypes;
        int[] execRowFormatIds;
        try {
            execRowFormatIds = getExecRowFormatIds(columnNames, nameTypes);
            keyColumnTypes = getKeyColumnTypes(keyColumnEncodingOrder,nameTypes);
        } catch (StandardException e) {
            throw new SQLException(e);
        }
        FormatableBitSet accessedKeyColumns = getAccessedKeyColumns(keyColumnEncodingOrder,keyDecodingMap);
        Txn txn=ReadOnlyTxn.create(Long.parseLong(getTransactionID()),IsolationLevel.SNAPSHOT_ISOLATION,null,HExceptionFactory.INSTANCE);
        TableScannerBuilder tableScannerBuilder = new SMTableBuilder();
        ExecRow template = WriteReadUtils.getExecRowFromTypeFormatIds(execRowFormatIds);
        return tableScannerBuilder
                .transaction(txn)
                .scan(createNewScan())
                .template(template)
                .tableVersion("2.0")
                .indexName(null)
                .keyColumnEncodingOrder(keyColumnEncodingOrder)
                .keyColumnSortOrder(null)
                .keyColumnTypes(keyColumnTypes)
                .accessedKeyColumns(accessedKeyColumns)
                .keyDecodingMap(keyDecodingMap)
                .keyColumnSortOrder(keyColumnOrdering)
                .rowDecodingMap(rowDecodingMap);
    }

    private static class SMTableBuilder extends TableScannerBuilder {
        public SMTableBuilder() {}

        @Override
        public DataSet buildDataSet() throws StandardException {
            throw new UnsupportedOperationException("We do not build data sets in this context.");
        }

        @Override
        protected DataScan readScan(ObjectInput in) throws IOException {
            return HOperationFactory.INSTANCE.readScan(in);
        }

        @Override
        public TxnView readTxn(ObjectInput oi) throws IOException {
            return new SimpleTxnOperationFactory(HExceptionFactory.INSTANCE, HOperationFactory.INSTANCE).readTxn(oi);
        }

        @Override
        protected void writeScan(ObjectOutput out) throws IOException {
            HOperationFactory.INSTANCE.writeScan(scan, out);
        }

        @Override
        protected void writeTxn(ObjectOutput out) throws IOException {
            new SimpleTxnOperationFactory(HExceptionFactory.INSTANCE, HOperationFactory.INSTANCE).writeTxn(txn, out);
        }
    }

    public void close() throws SQLException {
        if (connect!=null)
            connect.close();
    }

    public static DataScan createNewScan() {
        DataScan scan = HOperationFactory.INSTANCE.newScan();
        scan.returnAllVersions();
        return scan;
    }

    public Activation getActivation(String sql, TxnView txnView) throws SQLException, StandardException{

        PreparedStatement ps = connect.prepareStatement("call syscs_util.get_activation(?)");
        ps.setString(1, sql);
        ResultSet rs = ps.executeQuery();
        rs.next();
        byte[] activationHolderBytes = rs.getBytes(1);
        try {
            SpliceSpark.setupSpliceStaticComponents();
        } catch (IOException ioe) {
            StandardException.plainWrapException(ioe);
        }
        ActivationHolder ah = (ActivationHolder) SerializationUtils.deserialize(activationHolderBytes);
        ah.init(txnView);
        return ah.getActivation();
    }

}
