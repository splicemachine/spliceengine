package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.load.ColumnContext;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyPostfix;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.uuid.BasicUUIDGenerator;
import org.apache.log4j.Logger;

public class SMRecordWriterImpl extends RecordWriter<RowLocation, ExecRow> {
    static final Logger LOG = Logger.getLogger(SMRecordWriterImpl.class);
    protected TableContext tableContext;
    protected KeyEncoder keyEncoder;
    protected DataHash<ExecRow> dataHash;
    protected PairEncoder encoder;
    protected KVPair.Type pairType;
    protected int rowsWritten = 0;
    protected byte[] tableName;
    protected TxnView txn;
    protected RecordingCallBuffer<KVPair> callBuffer;
    protected int[] pkCols;
    protected int[] execRowFormatIds;
    protected SpliceSequence[] sequences;
    protected boolean hasSequence = false;
    protected ExecRow execRowDefn;
    protected SMSQLUtil sqlUtil;
    protected Connection conn;
    protected Configuration conf;
    protected long childTxsID;

    public SMRecordWriterImpl(TableContext tableContext, Configuration conf) throws IOException {
        this.tableContext = tableContext;
        this.pkCols = tableContext.pkCols;
        this.execRowFormatIds = tableContext.execRowFormatIds;
        ColumnContext[] columns = tableContext.columns;
        this.sequences = new SpliceSequence[columns.length];
        for(int i=0;i< columns.length;i++){
            ColumnContext cc = columns[i];
            if(columns[i].isAutoIncrement()){
                hasSequence = true;
                sequences[i] = new SpliceSequence(SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES),
                        50*cc.getAutoIncrementIncrement(),
                        cc.getSequenceRowLocation(),
                        cc.getAutoIncrementStart(),
                        cc.getAutoIncrementIncrement());
            }
        }
        execRowDefn = SMSQLUtil.getExecRow(execRowFormatIds);
        sqlUtil = SMSQLUtil.getInstance(conf.get(MRConstants.SPLICE_JDBC_STR));
        this.conf = conf;
    }


    @Override
    public void write(RowLocation ignore, ExecRow value) throws IOException,
            InterruptedException {
        try {
            if(callBuffer == null){
                conn = sqlUtil.createConn();
                sqlUtil.disableAutoCommit(conn);
                long parentTxnID = Long.parseLong(conf.get(MRConstants.SPLICE_TRANSACTION_ID));

                childTxsID = sqlUtil.getChildTransactionID(conn,
                        parentTxnID,
                        conf.get(MRConstants.SPLICE_TABLE_NAME));

                String strSize = conf.get(MRConstants.SPLICE_WRITE_BUFFER_SIZE);

                int size = 1024;
                if((strSize != null) && (!strSize.equals("")))
                    size = Integer.valueOf(strSize);

                txn = new ActiveWriteTxn(childTxsID,childTxsID);

                callBuffer = WriteCoordinator.create(conf).writeBuffer(Bytes.toBytes(Long.toString(tableContext.conglomerateId)),
                        txn, size);

                keyEncoder = getKeyEncoder();
                dataHash = getRowHash();

            }
            byte[] key = this.keyEncoder.getKey(value);
            dataHash.setRow(value);

            byte[] bdata = dataHash.encode();
            KVPair kv = new KVPair(key,bdata);
            SpliceLogUtils.error(LOG, "k = %s, v = %s", BytesUtil.toHex(key), BytesUtil.toHex(bdata));
            callBuffer.add(kv);
            callBuffer.flushBuffer();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        if(callBuffer == null){
            return;
        }
        try {
            this.callBuffer.close();
            sqlUtil.commitChildTransaction(conn, childTxsID);
            sqlUtil.commit(conn);
            sqlUtil.closeConn(conn);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            try {
                sqlUtil.rollback(conn);
                sqlUtil.closeConn(conn);
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                throw new IOException(e);
            }
        }

    }
    public KeyEncoder getKeyEncoder() throws StandardException, IOException {
        HashPrefix prefix;
        DataHash<ExecRow> dataHash;
        KeyPostfix postfix = NoOpPostfix.INSTANCE;
        if(pkCols==null || pkCols.length == 0){
            if (SpliceDriver.driver().getUUIDGenerator() == null) {
                SpliceDriver.driver().loadUUIDGenerator(1234);
            }
            prefix = new SaltedPrefix(SpliceDriver.driver().getUUIDGenerator().newGenerator(100));
            dataHash = NoOpDataHash.INSTANCE;
        }else{
            int[] keyColumns = new int[pkCols.length];
            for(int i=0;i<keyColumns.length;i++){
                keyColumns[i] = pkCols[i] -1;
            }

            prefix = NoOpPrefix.INSTANCE;
            DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(execRowDefn);
            dataHash = BareKeyHash.encoder(keyColumns,null,serializers);
        }

        return new KeyEncoder(prefix,dataHash,postfix);
    }

    public DataHash<ExecRow> getRowHash() throws StandardException {
        int[] columns = InsertOperation.getEncodingColumns(execRowDefn.nColumns(),pkCols);
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(execRowDefn);
        return new EntryDataHash(columns,null,serializers);
    }
}
