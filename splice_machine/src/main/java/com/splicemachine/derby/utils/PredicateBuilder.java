package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.io.Closeables;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.storage.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;
import org.apache.hadoop.hbase.filter.CompareFilter;

/**
 * @author Scott Fines
 * Date: 4/17/14
 */
public class PredicateBuilder {
		private final boolean[] keyColumnSortOrder;
		private final int[] keyEncodingMap;
		private final int[] columnTypes;
		private final String tableVersion;

		private DescriptorSerializer[] serializers;

		private ObjectArrayList<Predicate> predicates = ObjectArrayList.newInstance();

		public PredicateBuilder(int[] keyEncodingMap,
														boolean[] keyColumnSortOrder,
														int[] columnTypes,
														String tableVersion) {
				this.keyColumnSortOrder = keyColumnSortOrder;
				this.keyEncodingMap = keyEncodingMap;
				this.columnTypes = columnTypes;
				this.tableVersion = tableVersion;
		}

		public Predicate getPredicate(Qualifier qualifier) throws StandardException {
				DataValueDescriptor dvd = qualifier.getOrderable();
				if(dvd==null||dvd.isNull()||dvd.isNullOp().getBoolean()){
						boolean filterIfMissing = qualifier.negateCompareResult();
						boolean isNullValue = dvd==null||dvd.isNull();
						boolean isOrderedNulls = qualifier.getOrderedNulls();
						boolean isNullNumericalComparison = (isNullValue &&!isOrderedNulls);
						if(dvd==null)
							return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),false,false);
						else if(DerbyBytesUtil.isDoubleType(dvd))
								return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),true,false);
						else if(DerbyBytesUtil.isFloatType(dvd))
								return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),false,true);
						else
								return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),false,false);
				}else{
						boolean sort = getSortPosition(qualifier.getColumnId());
                        if(serializers==null)
                            serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(columnTypes);
                        byte[] bytes = serializers[qualifier.getColumnId()].encodeDirect(dvd, sort);

						if(dvd.getTypeFormatId() == StoredFormatIds.SQL_CHAR_ID){
								return new CharValuePredicate(getHBaseCompareOp(qualifier.getOperator(),
												qualifier.negateCompareResult()),qualifier.getColumnId(),bytes,true,sort);
						}else{
								return new ValuePredicate(getHBaseCompareOp(qualifier.getOperator(),qualifier.negateCompareResult()),
												qualifier.getColumnId(),
												bytes,true,sort);
						}
				}
		}

		private boolean getSortPosition(int columnRowPosition) {
				//in the case of primary keys, keyColumnSortOrder will be null (PKs are always ascending, as over v 0.5)
				if(keyColumnSortOrder==null) return false;
				else if(keyEncodingMap==null||keyEncodingMap.length<=0) return false;
				else{
						int keyPos = keyEncodingMap[columnRowPosition];
						return keyPos >= 0 && !keyColumnSortOrder[columnRowPosition];
				}
		}

		public Predicate build(){
				for(DescriptorSerializer serializer:serializers)
						Closeables.closeQuietly(serializer);
				serializers = null; //unlink the serializers to force new ones to be created if needed
				return AndPredicate.newAndPredicate(predicates);
		}

		private static CompareFilter.CompareOp getHBaseCompareOp(int operator, boolean negateResult) {
				if(negateResult){
						switch(operator){
								case DataValueDescriptor.ORDER_OP_EQUALS:
										return CompareFilter.CompareOp.NOT_EQUAL;
								case DataValueDescriptor.ORDER_OP_LESSTHAN:
										return CompareFilter.CompareOp.GREATER_OR_EQUAL;
								case DataValueDescriptor.ORDER_OP_GREATERTHAN:
										return CompareFilter.CompareOp.LESS_OR_EQUAL;
								case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
										return CompareFilter.CompareOp.GREATER;
								case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
										return CompareFilter.CompareOp.LESS;
								default:
										throw new AssertionError("Unknown Derby operator "+ operator);
						}
				}else{
						switch(operator){
								case DataValueDescriptor.ORDER_OP_EQUALS:
										return CompareFilter.CompareOp.EQUAL;
								case DataValueDescriptor.ORDER_OP_LESSTHAN:
										return CompareFilter.CompareOp.LESS;
								case DataValueDescriptor.ORDER_OP_GREATERTHAN:
										return CompareFilter.CompareOp.GREATER;
								case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
										return CompareFilter.CompareOp.LESS_OR_EQUAL;
								case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
										return CompareFilter.CompareOp.GREATER_OR_EQUAL;
								default:
										throw new AssertionError("Unknown Derby operator "+ operator);
						}
				}
		}
}
