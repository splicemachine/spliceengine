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
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.splicemachine.mrio.api.core.SMRecordReaderImpl;
import com.splicemachine.mrio.api.mapreduce.SMWrappedRecordReader;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;
/**
 * MR2 Input Format wrapping underling core SpliceInputFormat class.
 * 
 *
 */
public class SMInputFormat extends InputFormat<RowLocationWritable, ExecRowWritable> implements Configurable {
	protected com.splicemachine.mrio.api.core.SMInputFormat inputFormat;
	
	@Override
	public void setConf(Configuration conf) {
		if (inputFormat == null)
			inputFormat = new com.splicemachine.mrio.api.core.SMInputFormat();
		inputFormat.setConf(conf);
	}

	@Override
	public Configuration getConf() {
		return inputFormat.getConf();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		return inputFormat.getSplits(context);
	}

	@Override
	public RecordReader<RowLocationWritable, ExecRowWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new SMWrappedRecordReader((SMRecordReaderImpl)inputFormat.createRecordReader(split, context));
	}

}
