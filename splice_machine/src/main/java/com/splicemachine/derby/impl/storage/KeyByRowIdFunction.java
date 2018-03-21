package com.splicemachine.derby.impl.storage;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

/**
 * Created by jyuan on 2/6/18.
 */
public class KeyByRowIdFunction <Op extends SpliceOperation> extends SplicePairFunction<SpliceOperation,ExecRow,String,byte[]> {

    @Override
    public String genKey(ExecRow row) {
        return Bytes.toHex(row.getKey());
    }

    public byte[] genValue(ExecRow row) {
        return row.getKey();
    }

    @Override
    public Tuple2<String, byte[]> call(ExecRow execRow) throws Exception {
        return new Tuple2(genKey(execRow),genValue(execRow));
    }
}
