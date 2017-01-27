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

package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.Arrays;

import org.spark_project.guava.base.Throwables;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.altertable.AlterTableRowTransformer;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;

/**
 * Super class of alter table DDL descriptors. It exists mainly to house code
 * common to all alter table DDL descriptors.
 *
 * @author Jeff Cunningham
 *         Date: 4/28/15
 */
public abstract class AlterTableDDLDescriptor implements TransformingDDLDescriptor {
    protected final PipelineExceptionFactory exceptionFactory;

    public AlterTableDDLDescriptor(PipelineExceptionFactory exceptionFactory){
        this.exceptionFactory=exceptionFactory;
    }

    /**
     * Create encoders and decoders for alter table row transformer, then create
     * and return the row transformer.<br/>
     * This is code common to alter table DDL descriptors.
     * @param tableVersion the version of the tables in the transformation.
     * @param sourceKeyOrdering the key ordering of the source table.
     * @param targetKeyOrdering the key ordering of the target table.
     * @param columnMapping the one-based column mapping from source table to the
     *                      the target table, e.g.,
     *                      [1,2,4,6] indicates columns 3 and 5 were dropped and
     *                      the new table columns, which has 4 columns instead of 6,
     *                      should come from the given set of columns, in order they're
     *                      specified.
     * @param srcRow the source exec row template.
     * @param templateRow the target exec row template.
     * @param keyEncoder the key encoder to use when not dealing with PKs - key is HBase rowKey.
     * @return the row transformer initialized and ready to accept rows to transform.
     */
    protected static RowTransformer createRowTransformer(String tableVersion,
                                                         int[] sourceKeyOrdering,
                                                         int[] targetKeyOrdering,
                                                         int[] columnMapping,
                                                         ExecRow srcRow,
                                                         ExecRow templateRow,
                                                         KeyEncoder keyEncoder) {

        // Key decoder
        KeyHashDecoder keyDecoder;
        if(sourceKeyOrdering!=null && sourceKeyOrdering.length>0){
            // We'll need src table key column order when we have keys (PK, unique) on src table
            // Must use dense encodings in the key serializer (sparse = false)
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(srcRow);
            keyDecoder = BareKeyHash.decoder(sourceKeyOrdering, null, denseSerializers);
        }else{
            // Just use the no-op key decoder for src rows when no key in src table
            keyDecoder = NoOpDataHash.instance().getDecoder();
        }

        // Row decoder
        DescriptorSerializer[] oldSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(srcRow);
        EntryDataDecoder rowDecoder = new EntryDataDecoder(IntArrays.count(srcRow.nColumns()),null,oldSerializers);

        // Row encoder
        KeyEncoder encoder;
        DescriptorSerializer[] newSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(templateRow);
        if(targetKeyOrdering != null && targetKeyOrdering.length>0){
            // We'll need target table key column order when we have keys (PK, unique) on target table
            // Must use dense encodings in the key serializer (sparse = false)
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(templateRow);
            encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(targetKeyOrdering, null,
                                                                              denseSerializers), NoOpPostfix.INSTANCE);
        } else if (keyEncoder != null) {
            // Just use the no-op key decoder for new rows when no key in target table
            encoder = keyEncoder;
        } else {
            UUIDGenerator uuidGenerator = EngineDriver.driver().newUUIDGenerator(100);
            encoder = new KeyEncoder(new SaltedPrefix(uuidGenerator), NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }

        // Column ordering mask for all columns. Keys will be masked from the value row.
        int[] columnOrdering = IntArrays.count(templateRow.nColumns());
        if (targetKeyOrdering != null && targetKeyOrdering.length > 0) {
            for (int col: targetKeyOrdering) {
                columnOrdering[col] = -1;
            }
        }
        DataHash<ExecRow> rowHash = new EntryDataHash(columnOrdering, null,newSerializers);
        PairEncoder rowEncoder = new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);

        // Create and return the row transformer
        return new AlterTableRowTransformer(srcRow, columnMapping, templateRow, keyDecoder, rowDecoder, rowEncoder);

    }


    protected static FormatableBitSet getAccessedKeyColumns(int[] columnOrdering) {
        if (columnOrdering == null) return null; //no keys to decode

        FormatableBitSet accessedKeyCols = new FormatableBitSet(columnOrdering.length);
        /*
         * We need to access every column in the key
         */
        for (int i = 0; i < columnOrdering.length; i++) {
            accessedKeyCols.set(i);
        }
        return accessedKeyCols;
    }

    protected static int[] getKeyDecodingMap(FormatableBitSet pkCols, int[] baseColumnMap, int[] keyColumnEncodingOrder) {
        if (keyColumnEncodingOrder == null) {
            return new int[0];
        }

        int[] kDecoderMap = new int[keyColumnEncodingOrder.length];
        Arrays.fill(kDecoderMap, -1);
        for(int i=0; i<keyColumnEncodingOrder.length; i++){
            int baseKeyColumnPosition = keyColumnEncodingOrder[i]; //the position of the column in the base row
            if(pkCols.get(i)) {
                kDecoderMap[i] = baseColumnMap[baseKeyColumnPosition];
                baseColumnMap[baseKeyColumnPosition] = -1;
            } else {
                kDecoderMap[i] = -1;
            }
        }
        return kDecoderMap;
    }

    protected static int[] getKeyColumnTypes(ExecRow templateRow, int[] keyColumnEncodingOrder) throws IOException {
        if(keyColumnEncodingOrder==null) return null; //no keys to worry about
        int[] allFormatIds = getFormatIds(templateRow);
        int[] keyFormatIds = new int[keyColumnEncodingOrder.length];
        for(int i=0,pos=0; i<keyColumnEncodingOrder.length; i++){
            int keyColumnPosition = keyColumnEncodingOrder[i];
            if(keyColumnPosition>=0){
                keyFormatIds[pos] = allFormatIds[keyColumnPosition];
                pos++;
            }
        }
        return keyFormatIds;
    }

    protected static int[] getFormatIds(ExecRow templateRow) throws IOException {
        int[] formatIds = new int[templateRow.nColumns()];
        for (int i=0; i<templateRow.nColumns(); i++) {
            try {
                formatIds[i] = templateRow.getColumn(i+1).getTypeFormatId();
            } catch (StandardException e) {
                throw Exceptions.getIOException(Throwables.getRootCause(e));
            }
        }
        return formatIds;
    }

    protected static boolean[] getKeyColumnSortOrder(int len) {
        boolean[] keySortOrders = new boolean[len];
        for (int i=0; i<len; i++) {
            // FIXME: JC - sorting ascending always
            keySortOrders[i] = true;
        }
        return keySortOrders;
    }

    protected static int[] getRowDecodingMap(int len) {
        int[] rowDecodingMap = new int[len];
        for (int i=0; i<len; i++) {
            rowDecodingMap[i] = i;
        }
        return rowDecodingMap;
    }
}
