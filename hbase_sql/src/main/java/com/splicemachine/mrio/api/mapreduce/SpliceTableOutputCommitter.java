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
		// Create Sub Txn
	}

}
