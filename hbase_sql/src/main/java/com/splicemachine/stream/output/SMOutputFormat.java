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
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import scala.util.Either;

import java.io.IOException;

/**
 * Created by jleach on 5/18/15.
 */
public class SMOutputFormat extends OutputFormat<RowLocation,Either<Exception, ExecRow>> implements Configurable {
    private static Logger LOG = Logger.getLogger(SMOutputFormat.class);
    protected Configuration conf;
    protected SpliceOutputCommitter outputCommitter;
    //protected TxnView parentTxn;
    public SMOutputFormat() {
        super();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public RecordWriter<RowLocation,Either<Exception, ExecRow>> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            if (outputCommitter == null)
                getOutputCommitter(taskAttemptContext);
            DataSetWriterBuilder dsWriter = TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
            TxnView childTxn = outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID());
            if (childTxn == null)
                throw new IOException("child transaction lookup failed");
            dsWriter.txn(outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID()));
            return new SMRecordWriter(dsWriter.buildTableWriter(), outputCommitter);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"getOutputCommitter for taskAttemptContext=%s",taskAttemptContext);
        try {
            if (outputCommitter == null) {
                DataSetWriterBuilder tableWriter = TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
                outputCommitter = new SpliceOutputCommitter(tableWriter.getTxn(),tableWriter.getDestinationTable());
            }
            return outputCommitter;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

}
