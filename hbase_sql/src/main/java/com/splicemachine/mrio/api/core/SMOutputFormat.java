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

package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import com.splicemachine.mrio.api.mapreduce.SpliceTableOutputCommitter;

public class SMOutputFormat extends OutputFormat<RowLocation,ExecRow> implements Configurable {
    protected static final Logger LOG = Logger.getLogger(SMOutputFormat.class);
	protected Configuration conf;
	protected SMSQLUtil util;

	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "setConf conf=%s",conf);
	    String tableName = conf.get(MRConstants.SPLICE_OUTPUT_TABLE_NAME);
        if(tableName == null) {
            tableName = conf.get(MRConstants.SPLICE_TABLE_NAME);
        }
	    String conglomerate = conf.get(MRConstants.SPLICE_OUTPUT_CONGLOMERATE);
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
		
	    if (conglomerate == null) {
			try {
				conglomerate = util.getConglomID(tableName);
				conf.set(MRConstants.SPLICE_OUTPUT_CONGLOMERATE, conglomerate);
			} catch (SQLException e) {
				LOG.error(StringUtils.stringifyException(e));
				throw new RuntimeException(e);
			}		    	
	    }

        if (tableName != null) {
            tableName = tableName.trim().toUpperCase();
            List<String> colNames = new ArrayList<String>();
            try {
                if (!util.checkTableExists(tableName))
                    throw new SerDeException(String.format("table %s does not exist...", tableName));

                List<NameType> nameTypes = util.getTableStructure(tableName);
                for (NameType nameType : nameTypes) {
                    colNames.add(nameType.getName());
                }
                ScanSetBuilder tableScannerBuilder = util.getTableScannerBuilder(tableName, colNames);

            } catch (Exception e) {
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
//        TableContext tableContext = TableContext.getTableContextFromBase64String(context.getConfiguration().get(MRConstants.SPLICE_TBLE_CONTEXT));
        return new SMRecordWriterImpl(context.getConfiguration());
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
		return new SpliceTableOutputCommitter();
	}

}
