package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.SpliceDriver;
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
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
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
     * @return the row transformer initialized and ready to accept rows to transform.
     */
    protected static RowTransformer createRowTransformer(String tableVersion,
                                                      int[] sourceKeyOrdering,
                                                      int[] targetKeyOrdering,
                                                      int[] columnMapping,
                                                      ExecRow srcRow,
                                                      ExecRow templateRow) {

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
        if(targetKeyOrdering !=null&& targetKeyOrdering.length>0){
            // We'll need target table key column order when we have keys (PK, unique) on target table
            // Must use dense encodings in the key serializer (sparse = false)
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(templateRow);
            encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(targetKeyOrdering, null,
                                                                              denseSerializers), NoOpPostfix.INSTANCE);
        } else {
            // Just use the no-op key decoder for new rows when no key in target table
            UUIDGenerator uuidGenerator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
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
}
