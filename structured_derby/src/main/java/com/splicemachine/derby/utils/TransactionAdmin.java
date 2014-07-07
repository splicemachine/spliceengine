package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionStorage;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.impl.TransactionStore;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
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
						new GenericColumnDescriptor("allowsWrites",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
						new GenericColumnDescriptor("isAdditive",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
						new GenericColumnDescriptor("isolationLevel",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
						new GenericColumnDescriptor("commitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("effectiveCommitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("globalCommitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("status",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
						new GenericColumnDescriptor("lastKeepAlive",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP)),
						new GenericColumnDescriptor("modifiedConglomerate",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
		};

		private static final ResultColumnDescriptor[] CURRENT_TXN_ID_COLUMNS = new GenericColumnDescriptor[]{
						new GenericColumnDescriptor("txnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
		};

		public static void SYSCS_GET_CURRENT_TRANSACTION(ResultSet[] resultSet) throws SQLException{
				EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        Txn txn = ((SpliceTransactionManager)defaultConn.getLanguageConnection().getTransactionExecute()).getActiveStateTxn();
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

		public static void SYSCS_DUMP_TRANSACTIONS(ResultSet[] resultSet) throws SQLException {
        TxnStore store = TransactionStorage.getTxnStore();
        try{
            List<Txn> txns = store.getActiveTransactions(0,Long.MAX_VALUE,null);
            DataValueDescriptor[] templateCols = new DataValueDescriptor[TRANSACTION_TABLE_COLUMNS.length];
            for(int i=0;i<templateCols.length;i++){
                templateCols[i] = TRANSACTION_TABLE_COLUMNS[i].getType().getNull();
            }
            ExecRow template = new ValueRow(templateCols.length);
            template.setRowArray(templateCols);
            List<ExecRow> rows = Lists.newArrayListWithCapacity(txns.size());
						for(Txn txn:txns){
								template.resetRowArray();
								DataValueDescriptor[] dvds = template.getRowArray();
								dvds[0].setValue(txn.getTxnId());
								dvds[1].setValue(txn.getBeginTimestamp());
								if(txn.getParentTransaction()!=null && txn.getParentTransaction().getTxnId()!=-1l)
										dvds[2].setValue(txn.getParentTransaction().getTxnId());
								else
									dvds[2].setToNull();
								dvds[3].setValue(txn.isDependent());
								dvds[4].setValue(txn.allowsWrites());
								dvds[5].setValue(txn.isAdditive());
                dvds[6].setValue(txn.getIsolationLevel().toString());
								setLong(dvds[7], txn.getCommitTimestamp());
								setLong(dvds[8],txn.getEffectiveCommitTimestamp());
								setLong(dvds[9], txn.getGlobalCommitTimestamp());
								dvds[10].setValue(txn.getState().toString());
								dvds[11].setValue(new Timestamp(txn.getLastKeepAliveTimestamp()),null);
                Collection<byte[]> writeTables = txn.getDestinationTables();
                StringBuilder tables = new StringBuilder();
                boolean isFirst=true;
                for(byte[] table:writeTables){
                    if(!isFirst) tables.append(",");
                    else isFirst=false;
                    tables.append(Bytes.toString(table));
                }
								dvds[12].setValue(tables.toString());

								rows.add(template.getClone());
						}
						EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
						Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
						IteratorNoPutResultSet rs = new IteratorNoPutResultSet(rows,TRANSACTION_TABLE_COLUMNS,lastActivation);
						rs.openCore();
						resultSet[0] = new EmbedResultSet40(defaultConn,rs,false,null,true);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				} catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
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

}
