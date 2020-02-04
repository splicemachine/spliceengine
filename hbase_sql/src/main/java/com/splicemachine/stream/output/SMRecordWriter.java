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

package com.splicemachine.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.spark.SparkOperationContext;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
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
            OperationContext context = (OperationContext)tableWriter.getOperationContext();
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
