package com.splicemachine.mrio.api.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.load.ColumnInfo;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.DerbyDMLWriteInfo;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.VTIOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.DMLWriteInfo;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.derby.vti.SpliceIteratorVTI;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.splicemachine.mrio.api.core.SMRecordWriterImpl;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.mrio.api.serde.RowLocationWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class SMHiveRecordWriter implements RecordWriter<RowLocationWritable, ExecRowWritable> {

    protected static Logger LOG = Logger.getLogger(RecordWriter.class);
	protected Configuration conf;
    protected TxnView parentTxn;
    protected SMSQLUtil util;
    protected Activation activation;
    protected TxnView childTxn;
    protected Connection conn;

	public SMHiveRecordWriter (Configuration conf) {
		this.conf = conf;
        init();
	}
	
	@Override
	public void write(RowLocationWritable key, ExecRowWritable value)
			throws IOException {
        InsertOperation insertOperation = null;
        try {
            DataSet<LocatedRow> dataSet = getDataSet(value);
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

    private DataSet<LocatedRow> getDataSet(ExecRowWritable value) {
        HBaseRowLocation rowLocation = new HBaseRowLocation(Bytes.toBytes(1));
        LocatedRow locatedRow = new LocatedRow(rowLocation, value.get());
        ArrayList<LocatedRow> rows = Lists.newArrayList();
        rows.add(locatedRow);
        return new ControlDataSet(rows);
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
            childTxn = new ActiveWriteTxn(childTxsID,childTxsID);
            activation = util.getActivation(ceateVTIStatement(tableName), childTxn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String ceateVTIStatement(String fullTableName) throws SQLException{
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
        String sql = "insert into " + fullTableName +
                " select * from new com.splicemachine.derby.vti.SpliceIteratorVTI() as b (" + columnInfo.getImportAsColumns() + ")";

        return sql;
    }
}
