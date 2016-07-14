/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.storage.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/17/14
 */
public class PredicateBuilder{
    private final boolean[] keyColumnSortOrder;
    private final int[] keyEncodingMap;
    private final int[] columnTypes;
    private final String tableVersion;

    private DescriptorSerializer[] serializers;

    private ObjectArrayList<Predicate> predicates=ObjectArrayList.newInstance();

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public PredicateBuilder(int[] keyEncodingMap,
                            boolean[] keyColumnSortOrder,
                            int[] columnTypes,
                            String tableVersion){
        this.keyColumnSortOrder=keyColumnSortOrder;
        this.keyEncodingMap=keyEncodingMap;
        this.columnTypes=columnTypes;
        this.tableVersion=tableVersion;
    }

    public Predicate getPredicate(Qualifier qualifier) throws StandardException{
        DataValueDescriptor dvd=qualifier.getOrderable();
        int storagePosition = qualifier.getStoragePosition(); // physical column id, might differ from columnId
        if (storagePosition < 0) {
            throw new IllegalArgumentException(String.format("Qualifier has invalid storagePosition: %s", qualifier));
        }
        if(dvd==null || dvd.isNull() || dvd.isNullOp().getBoolean()){
            boolean filterIfMissing=qualifier.negateCompareResult();
            boolean isNullValue=dvd==null || dvd.isNull();
            boolean isOrderedNulls=qualifier.getOrderedNulls();
            boolean isNullNumericalComparison=(isNullValue && !isOrderedNulls);
            if(dvd==null)
                return new NullPredicate(filterIfMissing,isNullNumericalComparison,storagePosition,false,false);
            else if(DerbyBytesUtil.isDoubleType(dvd))
                return new NullPredicate(filterIfMissing,isNullNumericalComparison,storagePosition,true,false);
            else if(DerbyBytesUtil.isFloatType(dvd))
                return new NullPredicate(filterIfMissing,isNullNumericalComparison,storagePosition,false,true);
            else
                return new NullPredicate(filterIfMissing,isNullNumericalComparison,storagePosition,false,false);
        }else{
            // use columnId (not storagePosition) as index into these other maps
            boolean sort=getSortPosition(qualifier.getColumnId());
            if(serializers==null)
                serializers=VersionedSerializers.forVersion(tableVersion,true).getSerializers(columnTypes);
            byte[] bytes=serializers[qualifier.getColumnId()].encodeDirect(dvd,sort);

            if(dvd.getTypeFormatId() == StoredFormatIds.SQL_CHAR_ID){
                return new CharValuePredicate(getHBaseCompareOp(qualifier.getOperator(),
                        qualifier.negateCompareResult()),storagePosition,bytes,true,sort);
            }else{
                return new ValuePredicate(getHBaseCompareOp(qualifier.getOperator(),qualifier.negateCompareResult()),
                        storagePosition,
                        bytes,true,sort);
            }
        }
    }

    private boolean getSortPosition(int columnRowPosition){
        //in the case of primary keys, keyColumnSortOrder will be null (PKs are always ascending, as over v 0.5)
        if(keyColumnSortOrder==null) return false;
        else if(keyEncodingMap==null || keyEncodingMap.length<=0) return false;
        else{
            int keyPos=keyEncodingMap[columnRowPosition];
            return keyPos>=0 && !keyColumnSortOrder[columnRowPosition];
        }
    }

    public Predicate build(){
        for(DescriptorSerializer serializer : serializers)
            try{ serializer.close(); }catch(IOException ignored){ }
        serializers=null; //unlink the serializers to force new ones to be created if needed
        return AndPredicate.newAndPredicate(predicates);
    }

    private static CompareOp getHBaseCompareOp(int operator,boolean negateResult){
        if(negateResult){
            switch(operator){
                case DataValueDescriptor.ORDER_OP_EQUALS:
                    return CompareOp.NOT_EQUAL;
                case DataValueDescriptor.ORDER_OP_LESSTHAN:
                    return CompareOp.GREATER_OR_EQUAL;
                case DataValueDescriptor.ORDER_OP_GREATERTHAN:
                    return CompareOp.LESS_OR_EQUAL;
                case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
                    return CompareOp.GREATER;
                case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
                    return CompareOp.LESS;
                default:
                    throw new AssertionError("Unknown Derby operator "+operator);
            }
        }else{
            switch(operator){
                case DataValueDescriptor.ORDER_OP_EQUALS:
                    return CompareOp.EQUAL;
                case DataValueDescriptor.ORDER_OP_LESSTHAN:
                    return CompareOp.LESS;
                case DataValueDescriptor.ORDER_OP_GREATERTHAN:
                    return CompareOp.GREATER;
                case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
                    return CompareOp.LESS_OR_EQUAL;
                case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
                    return CompareOp.GREATER_OR_EQUAL;
                default:
                    throw new AssertionError("Unknown Derby operator "+operator);
            }
        }
    }
}
