package com.splicemachine.mrio.api.hive;

import java.io.IOException;

import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMRecordWriterImpl;
import com.splicemachine.mrio.api.core.TableContext;
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
	protected com.splicemachine.mrio.api.core.SMOutputFormat outputFormat;

    public SMHiveOutputFormat() {}

	public SMHiveOutputFormat(com.splicemachine.mrio.api.core.SMOutputFormat outputFormat) {
		this.outputFormat = outputFormat;
	}
	
	@Override
	public void setConf(Configuration conf) {
        if (outputFormat ==null)
            outputFormat = new com.splicemachine.mrio.api.core.SMOutputFormat();
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
        TableContext tableContext = TableContext.getTableContextFromBase64String(job.get(MRConstants.SPLICE_TBLE_CONTEXT));
        return new SMHiveRecordWriter(new SMRecordWriterImpl(tableContext, job));
	}

	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {
		// TODO Auto-generated method stub
		
	}


}
