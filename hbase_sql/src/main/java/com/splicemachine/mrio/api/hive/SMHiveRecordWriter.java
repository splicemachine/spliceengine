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
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.SQLException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.load.ColumnInfo;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.VTIOperation;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.vti.SpliceIteratorVTI;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class SMHiveRecordWriter implements RecordWriter<RowLocationWritable, ExecRowWritable> {

    protected static Logger LOG = Logger.getLogger(RecordWriter.class);
	protected Configuration conf;
    protected TxnView parentTxn;
    protected SMSQLUtil util;
    protected Activation activation;
    protected TxnView childTxn;
    protected Connection conn;

	public SMHiveRecordWriter (Configuration conf) throws IOException {
		this.conf = conf;
        String principal = conf.get("hive.server2.authentication.kerberos.principal");
        String keytab = conf.get("hive.server2.authentication.kerberos.keytab");
        if (principal != null && keytab != null) {
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            ugi.doAs(new PrivilegedAction<Void>(){
                public Void run() {
                    init();
                    return null;
                }
            });
        }
        else {
            init();
        }
	}
	
	@Override
	public void write(RowLocationWritable key, ExecRowWritable value)
			throws IOException {
        InsertOperation insertOperation = null;
        try {
            DataSet<ExecRow> dataSet = getDataSet(value);
            insertOperation = (InsertOperation)activation.execute();
            VTIOperation vtiOperation = (VTIOperation)getVTIOperation(insertOperation);
            SpliceIteratorVTI iteratorVTI = (SpliceIteratorVTI)vtiOperation.getDataSetProvider();
            iteratorVTI.setDataSet(dataSet);
            insertOperation.open();
            insertOperation.close();
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

	@Override
	public void close(Reporter reporter) throws IOException {
        try {
            util.commitChildTransaction(conn, childTxn.getBeginTimestamp());
            util.commit(conn);
            util.closeConn(conn);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            try {
                util.rollback(conn);
                util.closeConn(conn);
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                throw new IOException(e);
            }
        }
	}

    private SpliceOperation getVTIOperation(SpliceOperation op) {

        SpliceOperation spliceOperation = op;

        while(spliceOperation!= null && !(spliceOperation instanceof VTIOperation)) {
            spliceOperation = spliceOperation.getLeftOperation();
        }

        assert spliceOperation != null;
        return spliceOperation;
    }

    private DataSet<ExecRow> getDataSet(ExecRowWritable value) {
        return new ControlDataSet(new SingletonIterator(value.get()));
    }

    private void init() {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "setConf conf=%s", conf);
        String tableName = conf.get(MRConstants.SPLICE_OUTPUT_TABLE_NAME);
        if(tableName == null) {
            tableName = conf.get(MRConstants.SPLICE_TABLE_NAME).trim().toUpperCase();
        }

        if (tableName == null) {
            LOG.error("Table Name Supplied is null");
            throw new RuntimeException("Table Name Supplied is Null");
        }

        String jdbcString = conf.get(MRConstants.SPLICE_JDBC_STR);
        if (jdbcString == null) {
            LOG.error("JDBC String Not Supplied");
            throw new RuntimeException("JDBC String Not Supplied");
        }
        if (util==null)
            util = SMSQLUtil.getInstance(jdbcString);

        try {
            conn = util.createConn();
            if (!util.checkTableExists(tableName))
                throw new SerDeException(String.format("table %s does not exist...", tableName));
            long parentTxnID = Long.parseLong(conf.get(MRConstants.SPLICE_TRANSACTION_ID));
            long childTxsID = util.getChildTransactionID(conn, parentTxnID, tableName);
            // Assume default additivity and isolation levels. In the future if we want different isolation levels
            // we would need to pass in the whole transaction chain in the conf to this writer.
            parentTxn = new ActiveWriteTxn(parentTxnID, parentTxnID, Txn.ROOT_TRANSACTION, Txn.ROOT_TRANSACTION.isAdditive(), Txn.ROOT_TRANSACTION.getIsolationLevel());
            childTxn = new ActiveWriteTxn(childTxsID,childTxsID,parentTxn,parentTxn.isAdditive(),parentTxn.getIsolationLevel());
            activation = util.getActivation(createVTIStatement(tableName), childTxn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String createVTIStatement(String fullTableName) throws SQLException{
        String[] s = fullTableName.split("\\.");
        String schemaName = null;
        String tableName = null;
        if (s.length == 1) {
            tableName = s[0];
        }
        else {
            schemaName = s[0];
            tableName = s[1];
        }
        ColumnInfo columnInfo = new ColumnInfo(util.getStaticConnection(), schemaName, tableName, null);

        return "insert into " + fullTableName +
                " select * from new com.splicemachine.derby.vti.SpliceIteratorVTI() as b (" + columnInfo.getImportAsColumns() + ")";
    }
}
