package com.splicemachine.derby.impl.storage;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.kvpair.KVPair;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

/**
 * Created by jyuan on 2/6/18.
 */
public class KeyByBaseRowIdFunction <Op extends SpliceOperation> extends SplicePairFunction<SpliceOperation,ExecRow,String,byte[]> {

    @Override
    public String genKey(ExecRow row) {
        try {
            HBaseRowLocation rowLocation = (HBaseRowLocation) row.getColumn(row.nColumns());
            row.setColumn(row.nColumns(), rowLocation.cloneValue(true));
            return Bytes.toHex(rowLocation.getBytes());
        }catch (Exception e){
            throw new RuntimeException("Error generating key for " + row);
        }
    }

    public byte[] genValue(ExecRow row) {
        return row.getKey();
    }

    @Override
    public Tuple2<String, byte[]> call(ExecRow execRow) throws Exception {
        return new Tuple2(genKey(execRow),genValue(execRow));
    }
}
