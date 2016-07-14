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
