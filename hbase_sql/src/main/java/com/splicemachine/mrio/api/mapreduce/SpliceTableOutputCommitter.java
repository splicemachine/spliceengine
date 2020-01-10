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

package com.splicemachine.mrio.api.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SpliceTableOutputCommitter extends OutputCommitter {

	@Override
	public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
		// Rollback Txn...
	}

	@Override
	public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
		// Commit Txn...
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void setupJob(JobContext jobContext) throws IOException {
		
	}

	@Override
	public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
		// Create Sub Transaction
	}

}
