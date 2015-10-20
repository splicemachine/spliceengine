package com.splicemachine.derby.stream.function;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.hbase.KVPair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/16/15.
 */
public class IndexTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,KVPair,KVPair> {

    private static Logger LOG = Logger.getLogger(IndexTransformFunction.class);
    private boolean initialized;
    private IndexTransformer transformer;
    private boolean isUnique;
    private boolean isUniqueWithDuplicateNulls;
    private int[] indexColToMainColPosMap;
    private boolean[] descColumns;
    private int[] columnOrdering;
    private int[] format_ids;

    public IndexTransformFunction() {
        super();
    }

    public IndexTransformFunction(int[] indexColToMainColPosMap,
                                  boolean isUnique,
                                  boolean isUniqueWithDuplicateNulls,
                                  boolean[] descColumns,
                                  int[] columnOrdering,
                                  int[] format_ids) {

        this.indexColToMainColPosMap = indexColToMainColPosMap;
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.descColumns = descColumns;
        this.columnOrdering = columnOrdering;
        this.format_ids = format_ids;
        init();
    }

    @Override
    public KVPair call(KVPair mainKVPair) throws Exception {

        if (!initialized) {
            init();
        }

        //LOG.error("main row = " + BytesUtil.toHex(mainKVPair.getRowKey()));
        //LOG.error("main value = " + BytesUtil.toHex(mainKVPair.getValue()));

        KVPair indexKVPair = transformer.translate(mainKVPair);
        //LOG.error("index row = " + BytesUtil.toHex(indexKVPair.getRowKey()));
        //LOG.error("index value = " + BytesUtil.toHex(indexKVPair.getValue()));

        return indexKVPair;
    }

    private void init() {
        BitSet indexedColumns = new BitSet();
        for(int indexCol:indexColToMainColPosMap){
            indexedColumns.set(indexCol-1);
        }
        int[] mainColToIndexPosMap = new int[(int) indexedColumns.length()];
        for(int indexCol=0;indexCol<indexColToMainColPosMap.length;indexCol++){
            int mainCol = indexColToMainColPosMap[indexCol];
            mainColToIndexPosMap[mainCol-1] = indexCol;
        }
        BitSet descCols = new BitSet(descColumns.length);
        for(int col=0;col<descColumns.length;col++){
            if(descColumns[col])
                descCols.set(col);
        }
        transformer = new IndexTransformer(isUnique,
                isUniqueWithDuplicateNulls,
                null,
                columnOrdering,
                format_ids,
                null,
                mainColToIndexPosMap,
                descCols,
                indexedColumns);
        initialized = true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isUnique);
        out.writeBoolean(isUniqueWithDuplicateNulls);
        out.writeInt(indexColToMainColPosMap.length);
        for (int i = 0; i < indexColToMainColPosMap.length; ++i) {
            out.writeInt(indexColToMainColPosMap[i]);
        }

        out.writeInt(descColumns.length);
        for (int i = 0; i < descColumns.length; ++i) {
            out.writeBoolean(descColumns[i]);
        }

        out.writeInt(columnOrdering.length);
        for(int i = 0; i < columnOrdering.length; ++i) {
            out.writeInt(columnOrdering[i]);
        }

        out.writeInt(format_ids.length);
        for (int i = 0; i < format_ids.length; ++i) {
            out.writeInt(format_ids[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        isUnique = in.readBoolean();
        isUniqueWithDuplicateNulls = in.readBoolean();
        int len = in.readInt();
        indexColToMainColPosMap = new int[len];
        for (int i = 0; i < len; ++i) {
            indexColToMainColPosMap[i] = in.readInt();
        }

        len = in.readInt();
        descColumns = new boolean[len];
        for (int i = 0; i < len; ++i) {
            descColumns[i] = in.readBoolean();
        }

        len = in.readInt();
        columnOrdering = new int[len];
        for (int i = 0; i < len; ++i) {
            columnOrdering[i] = in.readInt();
        }

        len = in.readInt();
        format_ids = new int[len];
        for (int i = 0; i < len; ++i) {
            format_ids[i] = in.readInt();
        }
    }
}
