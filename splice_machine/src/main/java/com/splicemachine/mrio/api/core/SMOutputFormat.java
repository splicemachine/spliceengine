/**
 * SpliceOutputFormat which performs writing to Splice
 * @author Yanan Jian
 * Created on: 08/14/14
 */
package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.load.ColumnContext.Builder;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;


public class SMOutputFormat extends OutputFormat<RowLocation,ExecRow> implements Configurable {
    protected static final Logger LOG = Logger.getLogger(SMOutputFormat.class);
	protected Configuration conf;
	protected SMSQLUtil util;

	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "setConf conf=%s",conf);
	    String tableName = conf.get(MRConstants.SPLICE_TABLE_NAME);
	    String conglomerate = conf.get(MRConstants.SPLICE_CONGLOMERATE);
		String jdbcString = conf.get(MRConstants.SPLICE_JDBC_STR);
		if (tableName == null) {
		    LOG.error("Table Name Supplied is null");
	    	throw new RuntimeException("Table Name Supplied is Null");
	    }

		if (jdbcString == null) {
			LOG.error("JDBC String Not Supplied");
			throw new RuntimeException("JDBC String Not Supplied");
		}
		if (util==null)
			util = SMSQLUtil.getInstance(jdbcString);
		Map<String, Builder> columns = null;
		try {
			 columns = util.getColumns(tableName);
		} catch (SQLException sqle) {
			LOG.error(StringUtils.stringifyException(sqle));
			throw new RuntimeException(sqle);
		}
		
		
	    if (conglomerate == null) {
			try {
				conglomerate = util.getConglomID(tableName);
				conf.set(MRConstants.SPLICE_CONGLOMERATE, conglomerate);
			} catch (SQLException e) {
				LOG.error(StringUtils.stringifyException(e));
				throw new RuntimeException(e);
			}		    	
	    }

	    
	    
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public RecordWriter<RowLocation, ExecRow> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return null;
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
