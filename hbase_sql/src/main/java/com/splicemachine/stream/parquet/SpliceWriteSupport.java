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

package com.splicemachine.stream.parquet;

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
