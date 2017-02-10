/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
