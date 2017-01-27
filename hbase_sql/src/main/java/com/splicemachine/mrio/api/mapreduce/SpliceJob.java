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

package com.splicemachine.mrio.api.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSQLUtil;
/**
 * SpliceJob which controls submission of MapReduce Job
 * - Notice: You have to call commit() after SpliceJob finished successfully,
 *           You have to call rollback() after SpliceJob failed.
 *
 * @author Yanan Jian
 * Created on: 08/14/14
 */
public class SpliceJob extends Job {
	private static SMSQLUtil sqlUtil = null;
	private static Connection conn = null;
	private Configuration conf = null;

	public SpliceJob() throws IOException {
		super();
	}

	/**
	 * Construct SpliceJob
	 * 
	 * @param conf
	 * @throws IOException
	 */
	public SpliceJob(Configuration conf) throws IOException {
		super(conf, null);
		this.conf = conf;
	}

	/**
	 * Construct SpliceJob
	 * 
	 * @param conf
	 * @param jobName
	 * @throws IOException
	 */
	public SpliceJob(Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
		this.conf = conf;
	}

	/**
	 * Do not override this function! submit() creates one transaction for all
	 * mappers Thus data read from Splice is consistent.
	 */
	@Override
	public void submit() throws IOException, InterruptedException,
			ClassNotFoundException {
		if (sqlUtil == null)
			sqlUtil = SMSQLUtil.getInstance(super.getConfiguration().get(
					MRConstants.SPLICE_JDBC_STR));

		if (conn == null)
			try {
				conn = sqlUtil.createConn();
				sqlUtil.disableAutoCommit(conn);
				
				String pTxsID = sqlUtil.getTransactionID(conn);
				
				PreparedStatement ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_ELEVATE_TRANSACTION(?)");
		
				ps.setString(1,super.getConfiguration().get(MRConstants.SPLICE_OUTPUT_TABLE_NAME));
				ps.executeUpdate();

				super.getConfiguration().set(
						MRConstants.SPLICE_TRANSACTION_ID,
						String.valueOf(pTxsID));
				
			} catch (SQLException e1) {
				throw new IOException(e1);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		System.out.println("Submitting job...");
		super.submit();
		System.out.println("Submitted job...");

	}

	@Override
	public boolean waitForCompletion(boolean verbose) throws IOException,
			InterruptedException, ClassNotFoundException {
		return super.waitForCompletion(verbose);
	}

	/**
	 * Only when you call commit() can the transaction commit Every job starts
	 * as one transaction. It is important to call commit() explicitly in client
	 * code if job succeed.
	 * 
	 * @throws SQLException
	 */
	public void commit() throws SQLException {
		sqlUtil.commit(conn);
		sqlUtil.closeConn(conn);
		sqlUtil.close();
	}

	/**
	 * Only when you call rollback() can the transaction rollback Every job
	 * starts as one transaction. It is important to call rollback() explicitly
	 * in client code if job failed.
	 * 
	 * @throws SQLException
	 */
	public void rollback() throws SQLException {
		sqlUtil.rollback(conn);
		sqlUtil.closeConn(conn);
		sqlUtil.close();
	}

}
