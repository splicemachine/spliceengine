package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.file.DefaultFileInfo;
import com.splicemachine.utils.file.FileInfo;
import com.splicemachine.uuid.UUIDGenerator;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
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


		public static PairEncoder newEntryEncoder(ExecRow row,
                                              ImportContext ctx,
                                              UUIDGenerator randomGenerator,KVPair.Type importType){
				int[] pkCols = ctx.getPrimaryKeys();

				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(ctx.getTableVersion(), true).getSerializers(row);
				KeyEncoder encoder;
				if(pkCols!=null&& pkCols.length>0){
						encoder = new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(pkCols,null, serializers),NoOpPostfix.INSTANCE);
				}else
						encoder = new KeyEncoder(new SaltedPrefix(randomGenerator), NoOpDataHash.INSTANCE, NoOpPostfix.INSTANCE);

				int[] cols = IntArrays.count(row.nColumns());
				if (pkCols != null && pkCols.length>0) {
						for (int col:pkCols) {
								cols[col] = -1;
						}
				}
				DataHash rowHash = new EntryDataHash(cols,null,serializers);

				return new PairEncoder(encoder,rowHash, importType);
		}

		public static void buildColumnInformation(Connection connection,
																							 String schemaName,
																							 String tableName,
																							 String insertColumnList,
																							 ImportContext.Builder builder,
																							 byte[][] autoIncRowLocations,boolean upsert) throws SQLException {
				DatabaseMetaData dmd = connection.getMetaData();
				Map<String,ColumnContext.Builder> columns = getColumns(schemaName==null?"APP":schemaName.toUpperCase(),tableName.toUpperCase(),insertColumnList,dmd);

				//TODO -sf- this invokes an additional scan--is there any way that we can avoid this?
				try {
						computeAutoIncrementRowLocations(columns,autoIncRowLocations);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}

				Map<String,Integer> pkCols = getPrimaryKeys(schemaName, tableName, dmd);
        // as of 1.0.x (FUJI), Upserts are only allowed against tables which have a primary key
        if(upsert && (pkCols==null|| pkCols.size()<=0)){
            throw PublicAPI.wrapStandardException(ErrorState.UPSERT_NO_PRIMARY_KEYS.newException(schemaName+"."+tableName));
        }
				int[] pkKeyMap = new int[columns.size()];
				Arrays.fill(pkKeyMap, -1);
				for(String pkCol:pkCols.keySet()){
						columns.get(pkCol).primaryKeyPos(pkCols.get(pkCol));
        }
        if(insertColumnList!=null && !insertColumnList.equalsIgnoreCase("null")) {
            List<String> insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
            validateNonNullColumnsAreIncluded(insertCols, columns);

            for (ColumnContext.Builder colBuilder : columns.values()) {
                ColumnContext context = colBuilder.build();
                boolean found = false;
                int pos = 0;
                for(String insertCol:insertCols){
                    if (insertCol.equalsIgnoreCase(context.getColumnName())) {
                        context.setInsertPos(pos);
                        found = true;
                        break;
                    }
                    pos++;
                }
                if(found)
                    builder.addColumn(context);
            }
        } else{
            for(ColumnContext.Builder colBuilder:columns.values()){
                builder.addColumn(colBuilder.build());
            }
        }
    }

    private static void validateNonNullColumnsAreIncluded(List<String> insertCols,
                                                          Map<String,ColumnContext.Builder> columns) throws SQLException{
        /*
         * To ensure that non-null constraints are not violated, we must make sure that all columns which have a NOT_NULL
         * constraint attached to them are included in the columns list.
         *
         * We need to require this for both the IMPORT and UPSERT_FROM_FILE commands, but for different reasons:
         *
         * IMPORT requires it because we are assuming that we are creating a new record, so we need to ensure
         * that we are at the very least populating the NOT_NULL fields, and thus not violating our constraint.
         *
         * UPSERT_FROM_FILE could probably get away with not including the NOT_NULL fields *if it knew that all
         * rows already existed*--that is, if UPSERT_FROM_FILE was really an UPDATE. Since we can't make that
         * guarantee (we assume that we will perform at least some insertions), we have to ensure that
         * we are carrying along all of our NOT_NULL fields as well
         *
         * However, we can safely leave auto-increment columns alone, since they will auto-magically generated.
         */
        for(ColumnContext.Builder column:columns.values()){
            if(!column.isNullable() && !column.isAutoIncrement()){
                boolean found=false;
                for(String col:insertCols){
                    if(col.equalsIgnoreCase(column.getColumnName())){
                        found = true;
                        break;
                    }
                }
                if(!found)
                    throw PublicAPI.wrapStandardException(ErrorState.IMPORT_MISSING_NOT_NULL_KEY.newException(column.getColumnName()));
            }
        }
    }

    private static void computeAutoIncrementRowLocations(Map<String, ColumnContext.Builder> columns,
                                                         byte[][] rowLocationBytes) throws StandardException {
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
//						if(insertColumnList!=null && !insertColumnList.equalsIgnoreCase("null")){
//								List<String> insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
//								while(rs.next()){
//										ColumnContext.Builder colBuilder = buildColumn(rs);
//										String colName = colBuilder.getColumnName();
//										Iterator<String> colIterator = insertCols.iterator();
//										while(colIterator.hasNext()){
//												String insertCol = colIterator.next();
//												if(insertCol.equalsIgnoreCase(colName)){
//														columnMap.put(rs.getString(4),colBuilder);
//														colIterator.remove();
//														break;
//												}
//										}
//								}
//						}else{
								while(rs.next()){
										ColumnContext.Builder colBuilder = buildColumn(rs);

										columnMap.put(colBuilder.getColumnName(),colBuilder);
								}
//						}
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
				 if(colDefault!=null) {
	                  if(colType== Types.CHAR||colType==Types.VARCHAR||colType==Types.LONGVARCHAR) {
	                      if (colDefault.startsWith("\'") && colDefault.endsWith("\'")) {
	                          StringBuilder sb = new StringBuilder(colDefault);
	                          sb.deleteCharAt(0);
	                          sb.deleteCharAt(sb.length() - 1);
	                          colDefault = sb.toString();
	                      }
	                  }
	                }
				String isAutoIncrement = rs.getString(ISAUTOINCREMENT_POSIITON);
				boolean hasIncrementPrefix = colDefault!=null && colDefault.startsWith(AUTOINCREMENT_PREFIX);
				if (!"YES".equals(isAutoIncrement) || !hasIncrementPrefix) {
						colBuilder.columnDefault(colDefault);
				}else {
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

		public static void validateReadable(ImportFile file) throws StandardException{
				try {
						List<Path> paths = file.getPaths();
						FileSystem fs = file.getFileSystem();
						for(Path path:paths){
								validateReadable(path,fs,false);
						}
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		public static void validateReadable(Path path,FileSystem fileSystem,boolean checkDirectory) throws StandardException {
				//check that the badLogDirectory exists and is writable
				if(path!=null){
						FileInfo badLogInfo = new DefaultFileInfo(fileSystem);
						try{
								if(checkDirectory && !badLogInfo.isDirectory(path))
										throw ErrorState.LANG_FILE_DOES_NOT_EXIST.newException(path.toString());
								if(!badLogInfo.isReadable(path)){
										String[] ugi = badLogInfo.getUserAndGroup();
										throw ErrorState.LANG_NO_READ_PERMISSION.newException(ugi[0],ugi[1],path.toString());
								}
						}catch(FileNotFoundException fnfe){
								throw Exceptions.parseException(fnfe);
						}catch(IOException ioe){
								throw Exceptions.parseException(ioe);
						}
				}
		}

		public static void validateWritable(Path path,FileSystem fileSystem,boolean checkDirectory) throws StandardException {
				//check that the badLogDirectory exists and is writable
				if(path!=null){
						FileInfo badLogInfo = new DefaultFileInfo(fileSystem);
						try{
								if(checkDirectory && !badLogInfo.isDirectory(path))
										throw ErrorState.LANG_NOT_A_DIRECTORY.newException(path.toString());
								if(!badLogInfo.isWritable(path)){
										String[] ugi = badLogInfo.getUserAndGroup();
										throw ErrorState.LANG_NO_WRITE_PERMISSION.newException(ugi[0],ugi[1],path.toString());
								}
						}catch(FileNotFoundException fnfe){
								throw Exceptions.parseException(fnfe);
						}catch(IOException ioe){
								throw Exceptions.parseException(ioe);
						}
				}
		}
}
