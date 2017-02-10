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

package com.splicemachine.derby.utils;

import com.splicemachine.pipeline.ErrorState;
import org.spark_project.guava.collect.Lists;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.actions.ActiveTransactionReader;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/20/14
 */
public class TransactionAdmin{

    public static void killAllActiveTransactions(long maxTxnId) throws SQLException{
        ActiveTransactionReader reader=new ActiveTransactionReader(0l,maxTxnId,null);
        try(Stream<TxnView> activeTransactions=reader.getActiveTransactions()){
            final TxnLifecycleManager tc=SIDriver.driver().lifecycleManager();
            TxnView next;
            while((next=activeTransactions.next())!=null){
                tc.rollback(next.getTxnId());
            }
        }catch(StreamException|IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public static void killTransaction(long txnId) throws SQLException{
        try{
            TxnSupplier store=SIDriver.driver().getTxnStore();
            TxnView txn=store.getTransaction(txnId);
            //if the transaction is read-only, or doesn't exist, then don't do anything to it
            if(txn==null) return;

            TxnLifecycleManager tc=SIDriver.driver().lifecycleManager();
            tc.rollback(txnId);
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    private static final ResultColumnDescriptor[] CURRENT_TXN_ID_COLUMNS=new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("txnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    public static void SYSCS_GET_CURRENT_TRANSACTION(ResultSet[] resultSet) throws SQLException{
        EmbedConnection defaultConn=(EmbedConnection)SpliceAdmin.getDefaultConn();
        TxnView txn=((SpliceTransactionManager)defaultConn.getLanguageConnection().getTransactionExecute()).getActiveStateTxn();
        ExecRow row=new ValueRow(1);
        row.setColumn(1,new SQLLongint(txn.getTxnId()));
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet rs=new IteratorNoPutResultSet(Collections.singletonList(row),
                CURRENT_TXN_ID_COLUMNS,
                lastActivation);
        try{
            rs.openCore();
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
        resultSet[0]=new EmbedResultSet40(defaultConn,rs,false,null,true);
    }

    public static void SYSCS_GET_ACTIVE_TRANSACTION_IDS(ResultSet[] resultSets) throws SQLException{
        ActiveTransactionReader reader=new ActiveTransactionReader(0l,Long.MAX_VALUE,null);
        try{
            ExecRow template=toRow(CURRENT_TXN_ID_COLUMNS);
            List<ExecRow> results=Lists.newArrayList();
            try(Stream<TxnView> activeTxns=reader.getActiveTransactions()){
                TxnView n;
                while((n=activeTxns.next())!=null){
                    template.getColumn(1).setValue(n.getTxnId());
                    results.add(template.getClone());
                }
            }

            EmbedConnection defaultConn=(EmbedConnection)SpliceAdmin.getDefaultConn();
            Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet rs=new IteratorNoPutResultSet(results,CURRENT_TXN_ID_COLUMNS,lastActivation);
            rs.openCore();

            resultSets[0]=new EmbedResultSet40(defaultConn,rs,false,null,true);
        }catch(StreamException|IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
    }


    private static final ResultColumnDescriptor[] TRANSACTION_TABLE_COLUMNS=new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("txnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("parentTxnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("modifiedConglomerate",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("status",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("isolationLevel",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("beginTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("commitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("effectiveCommitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("isAdditive",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
            new GenericColumnDescriptor("lastKeepAlive",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP))
    };

    public static void SYSCS_DUMP_TRANSACTIONS(ResultSet[] resultSet) throws SQLException{
        throw PublicAPI.wrapStandardException(ErrorState.SPLICE_OPERATION_UNSUPPORTED.newException("CALL SYSCS_DUMP_TRANSACTIONS"));
//        ActiveTransactionReader reader=new ActiveTransactionReader(0l,Long.MAX_VALUE,null);
//        try{
//            ExecRow template=toRow(TRANSACTION_TABLE_COLUMNS);
//            List<ExecRow> results=Lists.newArrayList();
//
//            try(Stream<TxnView> activeTxns=reader.getAllTransactions()){
//                TxnView txn;
//                while((txn=activeTxns.next())!=null){
//                    template.resetRowArray();
//                    DataValueDescriptor[] dvds=template.getRowArray();
//                    dvds[0].setValue(txn.getTxnId());
//                    if(txn.getParentTxnId()!=-1l)
//                        dvds[1].setValue(txn.getParentTxnId());
//                    else
//                        dvds[1].setToNull();
//                    Iterator<ByteSlice> destTables=txn.getDestinationTables();
//                    if(destTables!=null && destTables.hasNext()){
//                        StringBuilder tables=new StringBuilder();
//                        boolean isFirst=true;
//                        while(destTables.hasNext()){
//                            ByteSlice table=destTables.next();
//                            if(!isFirst) tables.append(",");
//                            else isFirst=false;
//                            tables.append(Bytes.toString(Encoding.decodeBytesUnsortd(table.array(),table.offset(),table.length())));
//                        }
//                        dvds[2].setValue(tables.toString());
//                    }else
//                        dvds[2].setToNull();
//
//                    dvds[3].setValue(txn.getState().toString());
//                    dvds[4].setValue(txn.getIsolationLevel().toHumanFriendlyString());
//                    dvds[5].setValue(txn.getBeginTimestamp());
//                    setLong(dvds[6],txn.getCommitTimestamp());
//                    setLong(dvds[7],txn.getEffectiveCommitTimestamp());
//                    dvds[8].setValue(txn.isAdditive());
//                    dvds[9].setValue(new Timestamp(txn.getLastKeepAliveTimestamp()),null);
//                    results.add(template.getClone());
//                }
//            }
//            EmbedConnection defaultConn=(EmbedConnection)SpliceAdmin.getDefaultConn();
//            Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
//            IteratorNoPutResultSet rs=new IteratorNoPutResultSet(results,TRANSACTION_TABLE_COLUMNS,lastActivation);
//            rs.openCore();
//
//            resultSet[0]=new EmbedResultSet40(defaultConn,rs,false,null,true);
//        }catch(StreamException|IOException e){
//            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
//        }catch(StandardException e){
//            throw PublicAPI.wrapStandardException(e);
//        }
    }

    private static final ResultColumnDescriptor[] CHILD_TXN_ID_COLUMNS=new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("childTxnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    public static void SYSCS_COMMIT_CHILD_TRANSACTION(Long txnId) throws SQLException, IOException{
        TxnLifecycleManager tc=SIDriver.driver().lifecycleManager();
        TxnSupplier store=SIDriver.driver().getTxnStore();
        TxnView childTxn=store.getTransaction(txnId);
        if(childTxn==null){
            throw new IllegalArgumentException(String.format("Specified child transaction id %s not found.",txnId));
        }
        try{
            tc.commit(txnId);
        }catch(IOException e){

            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public static void SYSCS_ELEVATE_TRANSACTION(String tableName) throws IOException, SQLException{
        //TxnSupplier store = TransactionStorage.getTxnStore();
        EmbedConnection defaultConn=(EmbedConnection)SpliceAdmin.getDefaultConn();
        try{
            defaultConn.getLanguageConnection().getTransactionExecute().elevate(tableName);
        }catch(StandardException e){
            // TODO Auto-generated catch block
            throw new IllegalArgumentException(String.format("Specified tableName %s cannot be elevated. ",tableName));
        }

        ((SpliceTransactionManager)defaultConn.getLanguageConnection().getTransactionExecute()).getActiveStateTxn();

    }

    public static void SYSCS_START_CHILD_TRANSACTION(long parentTransactionId,String spliceTableName,ResultSet[] resultSet) throws IOException, SQLException{

        // Verify the parentTransactionId passed in
        TxnSupplier store=SIDriver.driver().getTxnStore();
        TxnView parentTxn=store.getTransaction(parentTransactionId);
        if(parentTxn==null){
            throw new IllegalArgumentException(String.format("Specified parent transaction id %s not found. Unable to create child transaction.",parentTransactionId));
        }

        Txn childTxn;
        try{
            childTxn=SIDriver.driver().lifecycleManager().beginChildTransaction(parentTxn,Bytes.toBytes(spliceTableName));
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }

        ExecRow row=new ValueRow(1);
        row.setColumn(1,new SQLLongint(childTxn.getTxnId()));
        EmbedConnection defaultConn=(EmbedConnection)SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet rs=new IteratorNoPutResultSet(Arrays.asList(row),CHILD_TXN_ID_COLUMNS,lastActivation);
        try{
            rs.openCore();
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
        resultSet[0]=new EmbedResultSet40(defaultConn,rs,false,null,true);
    }

    /******************************************************************************************************************/
    /*private helper methods*/
    private static void setLong(DataValueDescriptor dvd,Long value) throws StandardException{
        if(value!=null)
            dvd.setValue(value.longValue());
        else
            dvd.setToNull();
    }

    private static ExecRow toRow(ResultColumnDescriptor[] columns) throws StandardException{
        DataValueDescriptor[] dvds=new DataValueDescriptor[columns.length];
        for(int i=0;i<columns.length;i++){
            dvds[i]=columns[i].getType().getNull();
        }
        ExecRow row=new ValueRow(columns.length);
        row.setRowArray(dvds);
        return row;
    }
}
