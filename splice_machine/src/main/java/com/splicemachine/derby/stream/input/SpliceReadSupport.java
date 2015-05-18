package com.splicemachine.derby.stream.input;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.Map;

/**
 * Created by jleach on 5/15/15.
 */
public class SpliceReadSupport extends ReadSupport<ExecRow> {

    @Override
    public RecordMaterializer<ExecRow> prepareForRead(Configuration entries, Map<String, String> stringStringMap, MessageType messageType, ReadContext readContext) {
        return null;
    }
}
