package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.actions.ActiveTransactionReader;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.*;
import com.splicemachine.stream.CloseableStream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.utils.ByteSlice;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/20/14
 */
public class TransactionAdmin {
		private static final ResultColumnDescriptor[] TRANSACTION_TABLE_COLUMNS = new GenericColumnDescriptor[]{
						new GenericColumnDescriptor("txnId", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("beginTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("parentTxnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("isDependent",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
						new GenericColumnDescriptor("isAdditive",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
						new GenericColumnDescriptor("isolationLevel",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
						new GenericColumnDescriptor("commitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("effectiveCommitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("status",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
						new GenericColumnDescriptor("lastKeepAlive",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP)),
						new GenericColumnDescriptor("modifiedConglomerate",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
		};

		private static final ResultColumnDescriptor[] CURRENT_TXN_ID_COLUMNS = new GenericColumnDescriptor[]{
						new GenericColumnDescriptor("txnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
		};

		public static void SYSCS_GET_CURRENT_TRANSACTION(ResultSet[] resultSet) throws SQLException{
				EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        TxnView txn = ((SpliceTransactionManager)defaultConn.getLanguageConnection().getTransactionExecute()).getActiveStateTxn();
				ExecRow row = new ValueRow(1);
				row.setColumn(1,new SQLLongint(txn.getTxnId()));
				Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
				IteratorNoPutResultSet rs = new IteratorNoPutResultSet(Arrays.asList(row),CURRENT_TXN_ID_COLUMNS,lastActivation);
				try {
						rs.openCore();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
				resultSet[0] = new EmbedResultSet40(defaultConn,rs,false,null,true);
		}
    public static void SYSCS_GET_ACTIVE_TRANSACTION_IDS(ResultSet[] resultSets) throws SQLException{
        ActiveTransactionReader reader = new ActiveTransactionReader(0l,Long.MAX_VALUE,null);
        try {
            ExecRow template = toRow(CURRENT_TXN_ID_COLUMNS);
            List<ExecRow> results = Lists.newArrayList();
            CloseableStream<TxnView> activeTxns = reader.getActiveTransactions();
            try{
                TxnView n;
                while((n = activeTxns.next())!=null){
                    template.getColumn(1).setValue(n.getTxnId());
                    results.add(template.getClone());
                }
            }  finally{
                activeTxns.close();
            }

            EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
            Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet rs = new IteratorNoPutResultSet(results,CURRENT_TXN_ID_COLUMNS,lastActivation);
            rs.openCore();

            resultSets[0] = new EmbedResultSet40(defaultConn,rs,false,null,true);
        }catch(StreamException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static ExecRow toRow(ResultColumnDescriptor[] columns) throws StandardException {
        DataValueDescriptor[] dvds = new DataValueDescriptor[columns.length];
        for(int i=0;i<columns.length;i++){
            dvds[i] = columns[i].getType().getNull();
        }
        ExecRow row = new ValueRow(columns.length);
        row.setRowArray(dvds);
        return row;
    }
    
		public static void SYSCS_DUMP_TRANSACTIONS(ResultSet[] resultSet) throws SQLException {
        ActiveTransactionReader reader = new ActiveTransactionReader(0l,Long.MAX_VALUE,null);
        try {
            ExecRow template = toRow(TRANSACTION_TABLE_COLUMNS);
            List<ExecRow> results = Lists.newArrayList();
            CloseableStream<TxnView> activeTxns = reader.getAllTransactions();
            try{
                TxnView txn;
                while((txn = activeTxns.next())!=null){
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    dvds[0].setValue(txn.getTxnId());
                    dvds[1].setValue(txn.getBeginTimestamp());
                    if(txn.getParentTxnId()!=-1l)
                        dvds[2].setValue(txn.getParentTxnId());
                    else
                        dvds[2].setToNull();
                    dvds[3].setValue(txn.isDependent());
                    dvds[4].setValue(txn.isAdditive());
                    dvds[5].setValue(txn.getIsolationLevel().toHumanFriendlyString());
                    setLong(dvds[6], txn.getCommitTimestamp());
                    setLong(dvds[7],txn.getEffectiveCommitTimestamp());
                    dvds[8].setValue(txn.getState().toString());
                    dvds[9].setValue(new Timestamp(txn.getLastKeepAliveTimestamp()),null);
                    Iterator<ByteSlice> destTables = txn.getDestinationTables();
                    if(destTables!=null && destTables.hasNext()){
                        StringBuilder tables = new StringBuilder();
                        boolean isFirst=true;
                        while(destTables.hasNext()){
                            ByteSlice table = destTables.next();
                            if(!isFirst) tables.append(",");
                            else isFirst=false;
                            tables.append(Bytes.toString(Encoding.decodeBytesUnsortd(table.array(), table.offset(), table.length())));
                        }
                        dvds[10].setValue(tables.toString());
                    }else
                        dvds[10].setToNull();

                    results.add(template.getClone());
                }
            }  finally{
                activeTxns.close();
            }

            EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
            Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet rs = new IteratorNoPutResultSet(results,TRANSACTION_TABLE_COLUMNS,lastActivation);
            rs.openCore();

            resultSet[0] = new EmbedResultSet40(defaultConn,rs,false,null,true);
        }catch(StreamException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

		protected  static void setLong(DataValueDescriptor dvd, Long value) throws StandardException{
				if(value !=null)
						dvd.setValue(value.longValue());
				else
						dvd.setToNull();
		}
		protected static void setBoolean(DataValueDescriptor dvd, Boolean value) throws StandardException {
				if(value !=null)
						dvd.setValue(value.booleanValue());
				else
						dvd.setToNull();
		}

    private static final ResultColumnDescriptor[] CHILD_TXN_ID_COLUMNS = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("childTxnId", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    public static void SYSCS_START_CHILD_TRANSACTION(long parentTransactionId, ResultSet[] resultSet) throws IOException, SQLException {

        // Verify the parentTransactionId passed in
        TxnSupplier store = TransactionStorage.getTxnStore();
        TxnView parentTxn = store.getTransaction(parentTransactionId);
        if (parentTxn == null) {
            throw new IllegalArgumentException(String.format("Specified parent transaction id %s not found. Unable to create child transaction.", parentTransactionId));
        }

        //TODO -sf- this needs to add a destination table
        Txn childTxn;
        try {
            childTxn = TransactionLifecycle.getLifecycleManager().beginChildTransaction(parentTxn,null);
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }

        ExecRow row = new ValueRow(1);
        row.setColumn(1, new SQLLongint(childTxn.getTxnId()));
        EmbedConnection defaultConn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet rs = new IteratorNoPutResultSet(Arrays.asList(row), CHILD_TXN_ID_COLUMNS, lastActivation);
        try {
            rs.openCore();
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        resultSet[0] = new EmbedResultSet40(defaultConn, rs, false, null, true);
    }
}
