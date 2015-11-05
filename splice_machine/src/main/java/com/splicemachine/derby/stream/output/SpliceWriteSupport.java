package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import java.util.HashMap;

/**
 * Created by jleach on 5/15/15.
 */
public class SpliceWriteSupport extends WriteSupport<ExecRow> {
    protected ExecRowWriter execRowWriter;
    protected MessageType messageType;

    public SpliceWriteSupport() {

    }

    public SpliceWriteSupport(MessageType messageType) {
        this.messageType = messageType;
    }


    @Override
    public WriteContext init(Configuration conf) {
        return new WriteContext(messageType, new HashMap<String,String>()); // Do We need this?
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        execRowWriter = new ExecRowWriter((recordConsumer));
    }

    @Override
    public void write(ExecRow execRow) {
        try {
            execRowWriter.write(execRow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
