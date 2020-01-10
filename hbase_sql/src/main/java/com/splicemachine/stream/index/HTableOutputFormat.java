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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.stream.output.SpliceOutputCommitter;
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
 * Created by jyuan on 10/19/15.
 */
public class HTableOutputFormat extends OutputFormat<byte[],Either<Exception, KVPair>> implements Configurable {
    private static Logger LOG = Logger.getLogger(HTableOutputFormat.class);
    protected Configuration conf;
    protected SpliceOutputCommitter outputCommitter;

    public HTableOutputFormat() {
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
    public RecordWriter<byte[],Either<Exception, KVPair>> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            assert taskAttemptContext != null && taskAttemptContext.getConfiguration() != null:"configuration passed in is null";
            if (outputCommitter == null)
                getOutputCommitter(taskAttemptContext);
            DataSetWriterBuilder tableWriter =TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
            TxnView childTxn = outputCommitter.getChildTransaction(taskAttemptContext.getTaskAttemptID());
            if (childTxn == null)
                throw new IOException("child transaction lookup failed");
            tableWriter.txn(childTxn);
            return new HTableRecordWriter(tableWriter.buildTableWriter(), outputCommitter);
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
            SpliceLogUtils.debug(LOG, "getOutputCommitter for taskAttemptContext=%s", taskAttemptContext);
        try {
            if (outputCommitter == null) {
                DataSetWriterBuilder tableWriter =TableWriterUtils.deserializeTableWriter(taskAttemptContext.getConfiguration());
                outputCommitter = new SpliceOutputCommitter(tableWriter.getTxn(),tableWriter.getDestinationTable());
            }
            return outputCommitter;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }
}
