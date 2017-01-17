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

package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.storage.Record;
import org.spark_project.guava.primitives.Ints;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static org.spark_project.guava.base.Preconditions.checkArgument;

/**
 * Builds an index table KVPair given a base table KVPair.
 *
 * Transform:
 *
 * [srcRowKey, srcValue]  -> [indexRowKey, indexValue]
 *
 * Where
 *
 * FOR NON-UNIQUE: indexRowKey = [col1] + 0 + [col2] ... [colN] + encodeBytesUnsorted(srcRowKey)
 * FOR UNIQUE:     indexRowKey = [col1] + 0 + [col2] ... [colN]
 *
 * And
 *
 * indexValue = multiValueEncoding( encodeBytesUnsorted ( srcRowKey ))
 *
 * Where colN is an indexed column that may be encoded as part of srcRowKey or srcValue depending on if it is part of
 * a primary key in the source table.
 *
 * @author Scott Fines
 *         Date: 4/17/14
 */
@NotThreadSafe
public class IndexTransformer {
    private DDLMessage.Index index;
    private DDLMessage.Table table;
    private int [] mainColToIndexPosMap;
    private BitSet indexedCols;
    private byte[] indexConglomBytes;
    private int[] indexFormatIds;
    private transient Record baseResult = null;

    public IndexTransformer(DDLMessage.TentativeIndex tentativeIndex) {
        index = tentativeIndex.getIndex();
        table = tentativeIndex.getTable();
        checkArgument(!index.getUniqueWithDuplicateNulls() || index.getUniqueWithDuplicateNulls(), "isUniqueWithDuplicateNulls only for use with unique indexes");
        List<Integer> indexColsList = index.getIndexColsToMainColMapList();
        indexedCols = DDLUtils.getIndexedCols(Ints.toArray(indexColsList));
        List<Integer> allFormatIds = tentativeIndex.getTable().getFormatIdsList();
        mainColToIndexPosMap = DDLUtils.getMainColToIndexPosMap(Ints.toArray(index.getIndexColsToMainColMapList()), indexedCols);
        indexConglomBytes = DDLUtils.getIndexConglomBytes(index.getConglomerate());
        indexFormatIds = new int[indexColsList.size()];
        for (int i = 0; i < indexColsList.size(); i++) {
            indexFormatIds[i] = allFormatIds.get(indexColsList.get(i)-1);
        }
    }

    /**
     * @return true if this is a unique index.
     */
    public boolean isUniqueIndex() {
        return index.getUnique();
    }

    public BitSet gitIndexedCols() {
        return indexedCols;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public byte[] getIndexConglomBytes() {
        return indexConglomBytes;
    }
    /**
     * Create a KVPair that can be used to issue a delete on an index record associated with the given main
     * table mutation.
     * @param mutation the incoming modification. Its rowKey is used to get the the row in the base table
     *                 that will be updated. Once we have that, we can create a new KVPair of type
     *                 {@link KVPair.Type#DELETE DELETE} and call {@link #translate(KVPair)}
     *                 to translate it to the index's rowKey.
     * @param ctx the write context of the modification. Used to get transaction and region info.
     * @param indexedColumns the columns which are part of this index. Used to filter the Get.
     * @return An index row KVPair that can be used to delete the associated index row, or null if the mutated
     * row is not found (may have already been deleted).
     * @throws IOException for encoding/decoding problems.
     */
    public Record createIndexDelete(Record mutation, WriteContext ctx, BitSet indexedColumns) throws IOException {
        // do a Get() on all the indexed columns of the base table
        Record result =fetchBaseRow(mutation,ctx,indexedColumns);
//        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, new ObjectArrayList<Predicate>(),true);
//        get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
//        DataResult result = ctx.getRegion().get(get);
        if(result==null) // we can't find the old row, may have been deleted already
            return null;
        throw new UnsupportedOperationException("Not Supported");
//        return result.createIndexDelete(mainColToIndexPosMap,index.getUniqueWithDuplicateNulls(),index.getDescColumnsList().toArray(),null);
    }


    public Record writeDirectIndex(LocatedRow locatedRow) throws IOException, StandardException {
        assert locatedRow != null: "locatedRow passed in is null";
        ExecRow execRow = locatedRow.getRow();
        throw new NotSupportedException("Need to implemented exception");
    }


    /**
     * Translate the given base table record mutation into its associated, referencing index record.<br/>
     * Encapsulates the logic required to create an index record for a given base table record with
     * all the required discriminating and encoding rules (column is part of a PK, value is null, etc).
     * @param mutation KVPair containing the rowKey of the base table record for which we want to
     *                 translate to the associated index. This mutation should already have its requred
     *                 {@link KVPair.Type Type} set.
     * @return A KVPair representing the index record of the given base table mutation. This KVPair is
     * suitable for performing the required modification of the index record associated with this mutation.
     * @throws IOException for encoding/decoding problems.
     */
    public Record translate(Record mutation) throws IOException {
        throw new NotSupportedException("Not implemented yet");
//        mutation.createIndexInsert(mainColToIndexPosMap,index.getUniqueWithDuplicateNulls(),)
    }

    private Record fetchBaseRow(Record mutation,WriteContext ctx,BitSet indexedColumns) throws IOException{
        baseResult =ctx.getRegion().get(mutation.getKey(),ctx.getTxn(), IsolationLevel.SNAPSHOT_ISOLATION);
        return baseResult;
    }
}
