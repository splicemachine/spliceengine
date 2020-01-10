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

package com.splicemachine.mrio.api.hive;

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;

public class SMHiveOutputFormat implements OutputFormat<RowLocationWritable, ExecRowWritable>, Configurable {
	protected com.splicemachine.stream.output.SMOutputFormat outputFormat;

    public SMHiveOutputFormat() {}

	public SMHiveOutputFormat(com.splicemachine.stream.output.SMOutputFormat outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Override
	public void setConf(Configuration conf) {
        if (outputFormat ==null)
            outputFormat = new com.splicemachine.stream.output.SMOutputFormat();
		outputFormat.setConf(conf);
	}

	@Override
	public Configuration getConf() {
		return outputFormat.getConf();
	}

	@Override
	public RecordWriter<RowLocationWritable, ExecRowWritable> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
        return new SMHiveRecordWriter(job);
	}

	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {

	}


}
