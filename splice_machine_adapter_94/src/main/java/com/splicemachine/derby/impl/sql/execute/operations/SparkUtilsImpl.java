package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SparkUtils;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;

/**
 * Created by dgomezferro on 1/21/15.
 */
public class SparkUtilsImpl implements SparkUtils {
    @Override
    public ExecRow decode(ExecRow template, KeyHashDecoder decoder, Result result) {
//        Cell data = t._2().listCells().get(0);
//        decoder.set(CellUtils.getBuffer(data),data.getValueOffset(),data.getValueLength());
//        ExecRow row = template.getClone();
//        decoder.decode(row);
//        if (RDDUtils.LOG.isDebugEnabled()) {
//            RDDUtils.LOG.debug("Returning row: " + row + " with id " + System.identityHashCode(row));
//        }
        return null;
    }
}
