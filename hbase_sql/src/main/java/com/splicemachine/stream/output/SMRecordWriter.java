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

package com.splicemachine.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.spark.SparkOperationContext;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import scala.util.Either;

import java.io.IOException;

/**
 * Created by jleach on 5/18/15.
 */
public class SMRecordWriter extends RecordWriter<RowLocation,Either<Exception, ExecRow>> {
    private static Logger LOG = Logger.getLogger(SMRecordWriter.class);
    private boolean initialized = false;
    private TableWriter tableWriter;
    private OutputCommitter outputCommitter;
    private boolean failure = false;
    private ActivationHolder activationHolder;

    public SMRecordWriter(TableWriter tableWriter, OutputCommitter outputCommitter) throws StandardException{
        try {
            SpliceLogUtils.trace(LOG, "init");
            this.tableWriter = tableWriter;
            this.outputCommitter = outputCommitter;
            SparkOperationContext context = (SparkOperationContext)tableWriter.getOperationContext();
            if (context != null) {
                activationHolder = context.getActivationHolder();
                activationHolder.reinitialize(tableWriter.getTxn());
            }
        }
        catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(RowLocation rowLocation, Either<Exception, ExecRow> value) throws IOException, InterruptedException {
        if (value.isLeft()) {
            Exception e = value.left().get();
            // failure
            failure = true;
            SpliceLogUtils.error(LOG,"Error Reading",e);
            throw new IOException(e);
        }
        assert value.isRight();
        ExecRow execRow = value.right().get();
        try {
            if (!initialized) {
                initialized = true;
                tableWriter.open();
            }
            tableWriter.write(execRow);
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
            if (activationHolder != null) {
                activationHolder.close();
            }
            if (failure)
                outputCommitter.abortTask(taskAttemptContext);
        }
    }
}
