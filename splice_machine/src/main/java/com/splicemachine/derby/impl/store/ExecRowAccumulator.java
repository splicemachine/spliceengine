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

package com.splicemachine.derby.impl.store;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.EntryPredicateFilter;
import com.carrotsearch.hppc.BitSet;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 3/11/14
 */
public class ExecRowAccumulator extends ByteEntryAccumulator {
    protected final DataValueDescriptor[] dvds;
    protected final DescriptorSerializer[] serializers;
    protected final int[] columnMap;
    protected final int[] columnLengths;

    private ExecRowAccumulator(){
        super(null,false,null);
        this.dvds = null;
        this.serializers = null;
        this.columnMap = null;
        this.columnLengths = null;
    }

    private ExecRowAccumulator(EntryPredicateFilter predicateFilter,
                               boolean returnIndex,
                               BitSet fieldsToCollect,
                               DataValueDescriptor[] dvds,
                               int[] columnMap,
                               DescriptorSerializer[] serializers) {
        super(predicateFilter, returnIndex, fieldsToCollect);
        this.dvds = dvds;
        this.columnMap = columnMap;
        this.serializers = serializers;
        this.columnLengths = new int[dvds.length];
    }

    public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
                                                    boolean returnIndex,
                                                    ExecRow row,
                                                    int[] columnMap,
                                                    boolean[] columnSortOrder,
                                                    FormatableBitSet cols,
                                                    String tableVersion){
        DataValueDescriptor[] dvds = row.getRowArray();
        BitSet fieldsToCollect = new BitSet(dvds.length);
        boolean hasColumns = false;
        if(cols!=null){
            for(int i=cols.anySetBit();i>=0;i=cols.anySetBit(i)){
                hasColumns = true;
                fieldsToCollect.set(i);
            }
        }else if(columnMap!=null){
            for(int i=0;i<columnMap.length;i++){
                int pos = columnMap[i];
                if(pos<0) continue;
                hasColumns=true;
                if(dvds[pos]!=null)
                    fieldsToCollect.set(i);
            }
        }else{
            for(int i=0;i<dvds.length;i++){
                if(dvds[i]!=null){
                    hasColumns = true;
                    fieldsToCollect.set(i);
                }
            }
        }
        if(!hasColumns) return NOOP_ACCUMULATOR;

        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,false).getSerializers(row);
        if(columnSortOrder!=null)
            return new Ordered(predicateFilter,returnIndex,fieldsToCollect,dvds,columnMap,serializers,columnSortOrder);
        else
            return new ExecRowAccumulator(predicateFilter,returnIndex,fieldsToCollect,dvds,columnMap,serializers);
    }

    public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
                                                    boolean returnIndex,
                                                    ExecRow row,
                                                    int[] columnMap,
                                                    FormatableBitSet cols,
                                                    String tableVersion){
        return newAccumulator(predicateFilter,returnIndex,row,columnMap,null,cols,tableVersion);
    }

    public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
                                                    boolean returnIndex,
                                                    ExecRow row,
                                                    int[] keyColumns,
                                                    String tableVersion){
        return newAccumulator(predicateFilter,returnIndex,row,keyColumns,null,tableVersion);
    }

    @Override
    protected void occupy(int position, byte[] data, int offset, int length) {
        decode(position, data, offset, length);
        super.occupy(position,data,offset,length);
    }

    @Override
    protected void occupyDouble(int position, byte[] data, int offset, int length) {
        decode(position, data, offset, length);
        super.occupyDouble(position, data, offset, length);
    }

    @Override
    protected void occupyFloat(int position, byte[] data, int offset, int length) {
        decode(position, data, offset, length);
        super.occupyFloat(position, data, offset, length);
    }

    @Override
    protected void occupyScalar(int position, byte[] data, int offset, int length) {
        decode(position,data,offset,length);
        super.occupyScalar(position, data, offset, length);
    }

    @Override
    public byte[] finish() {
        return SIConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getCurrentLength(int position){
        int colPos = columnMap[position];
        return columnLengths[colPos];
    }

    protected void decode(int position, byte[] data, int offset, int length) {
        int colPos=columnMap[position];
        DataValueDescriptor dvd = dvds[colPos];
        DescriptorSerializer serializer = serializers[colPos];
        try {
            serializer.decodeDirect(dvd, data, offset, length, false);
            columnLengths[colPos] = length; //stored for future length measuring
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        for(DescriptorSerializer serializer:serializers){
            try{ serializer.close(); }catch(IOException ignored){ }
        }
    }

    private static class Ordered extends ExecRowAccumulator{

        private final boolean[] columnSortOrder;

        private Ordered(EntryPredicateFilter predicateFilter,
                        boolean returnIndex,
                        BitSet fieldsToCollect,
                        DataValueDescriptor[] dvds,
                        int[] columnMap,
                        DescriptorSerializer[] serializers,
                        boolean[] columnSortOrder) {
            super(predicateFilter, returnIndex, fieldsToCollect, dvds, columnMap, serializers);
            this.columnSortOrder = columnSortOrder;
        }

        @Override
        protected void decode(int position, byte[] data, int offset, int length) {
            int colPos=columnMap[position];
            DataValueDescriptor dvd = dvds[colPos];
            DescriptorSerializer serializer = serializers[columnMap[position]];
            try {
                serializer.decodeDirect(dvd, data, offset, length, !columnSortOrder[position]);
                columnLengths[colPos] = length;
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final ExecRowAccumulator NOOP_ACCUMULATOR = new ExecRowAccumulator(){
        @Override protected void decode(int position, byte[] data, int offset, int length) { }
        @Override protected void occupy(int position, byte[] data, int offset, int length) { }
        @Override protected void occupyDouble(int position, byte[] data, int offset, int length) { }
        @Override protected void occupyFloat(int position, byte[] data, int offset, int length) { }
        @Override protected void occupyScalar(int position, byte[] data, int offset, int length) { }
        @Override public void reset() { }

        @Override public boolean isFinished() { return true; }
    };

}
