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

package com.splicemachine.mrio.api.hive;

import java.io.IOException;
import java.util.List;

import com.splicemachine.mrio.api.core.SMRecordReaderImpl;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;
/**
 * MR2 Input Format wrapping underling core SpliceInputFormat class.
 * 
 *
 */
public class SMHiveInputFormat implements InputFormat<RowLocationWritable, ExecRowWritable>, Configurable {
	protected com.splicemachine.mrio.api.core.SMInputFormat inputFormat;

	@Override
	public void setConf(Configuration conf) {
		if (inputFormat ==null)
			inputFormat = new com.splicemachine.mrio.api.core.SMInputFormat();
		inputFormat.setConf(conf);
	}

	@Override
	public Configuration getConf() {
		return inputFormat.getConf();
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		try {
			List<org.apache.hadoop.mapreduce.InputSplit> splits = inputFormat.getSplits(new SMHIveContextWrapper(job));
			InputSplit[] returnSplits = new InputSplit[splits.size()];
			for (int i = 0; i< splits.size(); i++ ) {
				returnSplits[i] = new SMHiveSplit((SMSplit)splits.get(i), new Path("dummy"));
			}
			return returnSplits;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public RecordReader<RowLocationWritable, ExecRowWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		try {
			SMSplit smSplit = ((SMHiveSplit)split).getSMSplit();
			SMRecordReaderImpl delegate = inputFormat.getRecordReader(((SMHiveSplit) split).getSMSplit(), job);
			delegate.init(job, smSplit);
			return new SMHiveRecordReader(delegate);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

}
