package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.UUIDGenerator;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 2/26/14
 */
public class ImportUtils {
		private static final int COLTYPE_POSITION = 5;
		private static final int COLNAME_POSITION = 4;
		private static final int COLNULLABLE_POSITION = 11;
		private static final int COLSIZE_POSITION = 7;
		private static final int COLNUM_POSITION = 17;
		private static final int DECIMALDIGITS_POSIITON = 9;
		private static final int COLUMNDEFAULT_POSIITON = 13;
		private static final int ISAUTOINCREMENT_POSIITON = 23;
		private static final String AUTOINCREMENT_PREFIX = "AUTOINCREMENT: start ";


		public static PairEncoder newEntryEncoder(ExecRow row,ImportContext ctx, UUIDGenerator randomGenerator){
				return newEntryEncoder(row,ctx,randomGenerator,SpliceKryoRegistry.getInstance());
		}

		public static PairEncoder newEntryEncoder(ExecRow row,ImportContext ctx, UUIDGenerator randomGenerator,KryoPool kryoPool){
				int[] pkCols = ctx.getPrimaryKeys();

				KeyEncoder encoder;
				if(pkCols!=null&& pkCols.length>0){
						encoder = new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(pkCols,null),NoOpPostfix.INSTANCE);
				}else
						encoder = new KeyEncoder(new SaltedPrefix(randomGenerator), NoOpDataHash.INSTANCE, NoOpPostfix.INSTANCE);

				int[] cols = IntArrays.count(row.nColumns());
				if (pkCols != null && pkCols.length>0) {
						for (int col:pkCols) {
								cols[col] = -1;
						}
				}
				DataHash rowHash = new EntryDataHash(cols,null);

				return new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
		}

		public static void buildColumnInformation(Connection connection,
																							 String schemaName,
																							 String tableName,
																							 String insertColumnList,
																							 ImportContext.Builder builder,
																							 byte[][] autoIncRowLocations) throws SQLException {
				DatabaseMetaData dmd = connection.getMetaData();
				Map<String,ColumnContext.Builder> columns = getColumns(schemaName==null?"APP":schemaName.toUpperCase(),tableName.toUpperCase(),insertColumnList,dmd);

				//TODO -sf- this invokes an additional scan--is there any way that we can avoid this?
//				DataDictionary dataDictionary = lcc.getDataDictionary();
//				TransactionController tc = lcc.getTransactionExecute();
				try {
//						SchemaDescriptor sd = dataDictionary.getSchemaDescriptor(schemaName, tc,true);
//						if(sd==null)
//								throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName));
//						TableDescriptor td = dataDictionary.getTableDescriptor(tableName,sd, tc);
//						if(td==null)
//								throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
//						long conglomerateId = td.getHeapConglomerateId();
//						builder.destinationTable(conglomerateId);
						computeAutoIncrementRowLocations(columns,autoIncRowLocations);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}

				Map<String,Integer> pkCols = getPrimaryKeys(schemaName, tableName, dmd);
				int[] pkKeyMap = new int[columns.size()];
				Arrays.fill(pkKeyMap, -1);
				for(String pkCol:pkCols.keySet()){
						columns.get(pkCol).primaryKeyPos(pkCols.get(pkCol));
				}

				for(ColumnContext.Builder colBuilder:columns.values()){
						builder.addColumn(colBuilder.build());
				}
		}

		private static void computeAutoIncrementRowLocations(Map<String, ColumnContext.Builder> columns,
																												 byte[][] rowLocationBytes) throws StandardException {
//				RowLocation[] rowLocations = dataDictionary.computeAutoincRowLocations(tc, td);

				for(ColumnContext.Builder cb:columns.values()){
						if(cb.isAutoIncrement()){
								cb.sequenceRowLocation(rowLocationBytes[cb.getColumnNumber()]);
						}
				}
		}

		private static Map<String,ColumnContext.Builder> getColumns(String schemaName, String tableName,
																																String insertColumnList, DatabaseMetaData dmd) throws SQLException{
				ResultSet rs = null;
				Map<String,ColumnContext.Builder> columnMap = Maps.newHashMap();
				try{
						rs = dmd.getColumns(null,schemaName,tableName,null);
						if(insertColumnList!=null && !insertColumnList.equalsIgnoreCase("null")){
								List<String> insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
								while(rs.next()){
										ColumnContext.Builder colBuilder = buildColumn(rs);
										String colName = colBuilder.getColumnName();
										Iterator<String> colIterator = insertCols.iterator();
										while(colIterator.hasNext()){
												String insertCol = colIterator.next();
												if(insertCol.equalsIgnoreCase(colName)){
														columnMap.put(rs.getString(4),colBuilder);
														colIterator.remove();
														break;
												}
										}
								}
						}else{
								while(rs.next()){
										ColumnContext.Builder colBuilder = buildColumn(rs);

										columnMap.put(colBuilder.getColumnName(),colBuilder);
								}
						}
						return columnMap;
				}finally{
						if(rs!=null)rs.close();
				}

		}

		private static ColumnContext.Builder buildColumn(ResultSet rs) throws SQLException {
				ColumnContext.Builder colBuilder = new ColumnContext.Builder();
				String colName = rs.getString(COLNAME_POSITION);
				colBuilder.columnName(colName);
				int colPos = rs.getInt(COLNUM_POSITION);
				colBuilder.columnNumber(colPos-1);
				int colType = rs.getInt(COLTYPE_POSITION);
				colBuilder.columnType(colType);
				boolean isNullable = rs.getInt(COLNULLABLE_POSITION)!=0;
				colBuilder.nullable(isNullable);
				if(colType== Types.CHAR||colType==Types.VARCHAR||colType==Types.LONGVARCHAR||colType == Types.DECIMAL){
						int colSize = rs.getInt(COLSIZE_POSITION);
						colBuilder.length(colSize);
				}
				if (colType == Types.DECIMAL)
				{
						int decimalDigits = rs.getInt(DECIMALDIGITS_POSIITON);
						colBuilder.decimalDigits(decimalDigits);
				}
				/*
				 * The COLUMNDEFAULT position contains two separate entities: a default value (if the column
				 * has a default) or a String that looks like AUTOINCREMENT: start x increment y. We need to
				 * deal with each case separately
				 */
				String colDefault = rs.getString(COLUMNDEFAULT_POSIITON);
				String isAutoIncrement = rs.getString(ISAUTOINCREMENT_POSIITON);
				boolean hasIncrementPrefix = colDefault!=null && colDefault.startsWith(AUTOINCREMENT_PREFIX);
				if (isAutoIncrement.compareTo("YES") != 0 || !hasIncrementPrefix) {
						colBuilder.columnDefault(colDefault);
				}else if (hasIncrementPrefix){
						//colDefault looks like "AUTOINCREMENT: start x increment y
						colDefault = colDefault.substring(colDefault.indexOf('s'));
						int endIndex = colDefault.indexOf(' ', 6);
						long startVal = Long.parseLong(colDefault.substring(6, endIndex).trim());
						colDefault = colDefault.substring(endIndex);
						long incVal = Long.parseLong(colDefault.substring(10).trim());
						colBuilder.autoIncrementStart(startVal).autoIncrementIncrement(incVal);
				}
				return colBuilder;
		}

		private static Map<String,Integer> getPrimaryKeys(String schemaName, String tableName,
																											DatabaseMetaData dmd) throws SQLException {
				//get primary key information
				ResultSet rs = null;
				try{
						rs = dmd.getPrimaryKeys(null,schemaName,tableName.toUpperCase());
						Map<String,Integer> pkCols = Maps.newHashMap();
						while(rs.next()){
                /*
                 * The column number of use is the KEY_SEQ field in the returned result,
                 * which is one-indexed. For convenience, we adjust it to be zero-indexed here.
                 */
								pkCols.put(rs.getString(4), rs.getShort(5) - 1);
						}
						return pkCols;
				}finally{
						if(rs!=null)rs.close();
				}
		}
}
