package com.splicemachine.derby.hbase;

import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;

/**
 * Created by dgomezferro on 1/21/15.
 */
public interface SparkUtils {
    ExecRow decode(ExecRow template, KeyHashDecoder decoder, Result result) throws StandardException;
}
