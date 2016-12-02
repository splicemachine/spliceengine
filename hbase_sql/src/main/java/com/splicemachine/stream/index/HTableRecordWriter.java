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

package com.splicemachine.stream.index;

import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import scala.util.Either;

import java.io.IOException;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableRecordWriter extends RecordWriter<byte[],Either<Exception, KVPair>> {
    private static Logger LOG = Logger.getLogger(HTableRecordWriter.class);
    boolean initialized = false;
    TableWriter tableWriter;
    OutputCommitter outputCommitter;
    private boolean failure = false;

    public HTableRecordWriter(TableWriter tableWriter, OutputCommitter outputCommitter) {
        SpliceLogUtils.trace(LOG, "init");
        this.tableWriter = tableWriter;
        this.outputCommitter = outputCommitter;
    }

    @Override
    public void write(byte[] rowKey, Either<Exception, KVPair> value) throws IOException, InterruptedException {
        if (value.isLeft()) {
            Exception e = value.left().get();
            // failure
            failure = true;
            SpliceLogUtils.error(LOG,"Error Reading",e);
            throw new IOException(e);
        }
        assert value.isRight();
        KVPair kvPair = value.right().get();
        try {
            if (!initialized) {
                initialized = true;
                tableWriter.open();
            }
            tableWriter.write(kvPair);
        } catch (Exception se) {
            SpliceLogUtils.error(LOG,"Error Writing",se);
            failure = true;
            throw new IOException(se);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        SpliceLogUtils.trace(LOG,"closing %s",taskAttemptContext);
        try {
            if (initialized) {
                tableWriter.close();
            }
        } catch (Exception e) {
            failure = true;
            throw new IOException(e);
        } finally {
            if (failure)
                outputCommitter.abortTask(taskAttemptContext);
        }
    }
}
