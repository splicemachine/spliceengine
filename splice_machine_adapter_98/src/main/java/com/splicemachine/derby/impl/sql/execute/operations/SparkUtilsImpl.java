package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SparkUtils;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

/**
 * Created by dgomezferro on 1/21/15.
 */
public class SparkUtilsImpl implements SparkUtils {
    @Override
    public ExecRow decode(ExecRow template, KeyHashDecoder decoder, Result result) throws StandardException {
        Cell data = result.listCells().get(0);
        decoder.set(CellUtils.getBuffer(data),data.getValueOffset(),data.getValueLength());
        ExecRow row = template.getClone();
        decoder.decode(row);
        return row;
    }
}
