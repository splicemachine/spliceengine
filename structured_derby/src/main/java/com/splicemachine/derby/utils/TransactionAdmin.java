package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.HTransactorFactory;
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
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
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
						new GenericColumnDescriptor("readUncommitted",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
						new GenericColumnDescriptor("readCommitted",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
						new GenericColumnDescriptor("commitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("effectiveCommitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("globalCommitTimestamp",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("status",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
						new GenericColumnDescriptor("lastKeepAlive",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP)),
						new GenericColumnDescriptor("counter",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
						new GenericColumnDescriptor("modifiedConglomerate",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
		};

		private static final ResultColumnDescriptor[] CURRENT_TXN_ID_COLUMNS = new GenericColumnDescriptor[]{
						new GenericColumnDescriptor("txnId",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
		};

		public static void SYSCS_GET_CURRENT_TRANSACTION(ResultSet[] resultSet) throws SQLException{
				EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
				TransactionId txnId= new TransactionId(getTransactionId(defaultConn.getLanguageConnection().getTransactionExecute()));
				ExecRow row = new ValueRow(1);
				row.setColumn(1,new SQLLongint(txnId.getId()));
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
				try {
						TransactionStore store = HTransactorFactory.getTransactionStore();
						//noinspection unchecked
						List<Transaction> transactions =store.getAllTransactions();
						ExecRow template = new ValueRow(15);
						template.setRowArray(new DataValueDescriptor[]{
										new SQLLongint(), //txnId
										new SQLLongint(), //begin
										new SQLLongint(), //parent id
										new SQLBoolean(), //dependent
										new SQLBoolean(), //allowsWrites
										new SQLBoolean(), //isAdditive
										new SQLBoolean(), //readUncommitted
										new SQLBoolean(), //readCommitted
										new SQLLongint(),  //commit timestamp
										new SQLLongint(), //effective commit timestamp
										new SQLLongint(), //global commit timestamp
										new SQLVarchar(), //status
										new SQLTimestamp(), //last keep alive
										new SQLInteger(), //counter
										new SQLVarchar() //destinationConglomerate
						});

						List<ExecRow> rows = Lists.newArrayListWithCapacity(transactions.size());
						for(Transaction txn:transactions){
								template.resetRowArray();
								DataValueDescriptor[] dvds = template.getRowArray();
								dvds[0].setValue(txn.getLongTransactionId());
								dvds[1].setValue(txn.getBeginTimestamp());
								if(txn.getParent()!=null && txn.getParent().getLongTransactionId()!=-1l)
										dvds[2].setValue(txn.getParent().getLongTransactionId());
								else
									dvds[2].setToNull();
								dvds[3].setValue(txn.isDependent());
								dvds[4].setValue(txn.allowsWrites());
								dvds[5].setValue(txn.isAdditive());
								setBoolean(dvds[6], txn.getReadUncommitted());
								setBoolean(dvds[7], txn.getReadCommitted());
								setLong(dvds[8], txn.getCommitTimestampDirect());
								setLong(dvds[9],txn.getEffectiveCommitTimestamp());
								setLong(dvds[10], txn.getGlobalCommitTimestamp());
								dvds[11].setValue(txn.getStatus().toString());
								dvds[12].setValue(new Timestamp(txn.getKeepAlive()),null);
								dvds[13].setValue(txn.getCounter());
								dvds[14].setValue(Bytes.toString(txn.getWriteTable()));

								rows.add(template.getClone());
						}

						EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
						Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
						IteratorNoPutResultSet rs = new IteratorNoPutResultSet(rows,TRANSACTION_TABLE_COLUMNS,lastActivation);
						rs.openCore();
						resultSet[0] = new EmbedResultSet40(defaultConn,rs,false,null,true);
				} catch (IOException e) {
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

		private static String getTransactionId(TransactionController tc) {
				org.apache.derby.iapi.store.raw.Transaction td = ((SpliceTransactionManager)tc).getRawTransaction();
				return SpliceUtils.getTransID(td);
		}
}
