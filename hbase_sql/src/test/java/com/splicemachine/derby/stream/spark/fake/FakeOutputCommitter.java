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

package com.splicemachine.derby.stream.spark.fake;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.output.SpliceOutputCommitter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.File;
import java.io.IOException;

public class FakeOutputCommitter extends SpliceOutputCommitter {

    public FakeOutputCommitter() {
        super(Txn.ROOT_TRANSACTION, new byte[0]);
    }

    @Override
    public TxnView getChildTransaction(TaskAttemptID taskAttemptID) {
        return parentTxn;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return true;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
        String abortDirectory = taskAttemptContext.getConfiguration().get("abort.directory");
        File file = new File(abortDirectory, taskAttemptContext.getTaskAttemptID().getTaskID().toString());
        file.createNewFile();
    }

}
