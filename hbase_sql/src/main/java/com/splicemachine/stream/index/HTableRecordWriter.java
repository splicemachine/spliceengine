/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
