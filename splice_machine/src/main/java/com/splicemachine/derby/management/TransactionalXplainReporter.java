package com.splicemachine.derby.management;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.pipeline.utils.PipelineConstants;
import com.splicemachine.si.api.ReadOnlyModificationException;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 9/11/14
 */
public abstract class TransactionalXplainReporter<T> {
    private static final String SCHEMA = "SYS";
    private final String tableName;
    private volatile String conglomIdString;

    private final ThreadLocal<Pair<DataHash<T>,DataHash<T>>> hashLocals;

    protected TransactionalXplainReporter(final String tableName) throws StandardException {
        this.tableName = tableName;
        this.hashLocals = new ThreadLocal<Pair<DataHash<T>, DataHash<T>>>(){
            @Override
            protected Pair<DataHash<T>, DataHash<T>> initialValue() {
                return Pair.newPair(getKeyHash(), getDataHash());
            }
        };
    }

    public void initialize(LanguageConnectionContext lcc) throws IOException {
        getConglomIdString();
    }

    protected abstract DataHash<T> getDataHash();

    protected abstract DataHash<T> getKeyHash();

    public void report(T element,TxnView txn) throws IOException{
        if(!txn.allowsWrites())
            throw new ReadOnlyModificationException("Cannot report xplain data with a read-only transaction "+ txn.getTxnId());
        Pair<DataHash<T>,DataHash<T>> hashPair = hashLocals.get();
        DataHash<T> keyHash = hashPair.getFirst();
        DataHash<T> pairHash = hashPair.getSecond();

        keyHash.setRow(element);
        pairHash.setRow(element);

        String conglom = getConglomIdString();
        WriteCoordinator tableWriter = SpliceDriver.driver().getTableWriter();
        RecordingCallBuffer<KVPair> callBuffer = tableWriter.synchronousWriteBuffer(conglom.getBytes(),
                txn, PipelineConstants.noOpFlushHook, tableWriter.defaultWriteConfiguration());
        try{
            callBuffer.add(new KVPair(keyHash.encode(),pairHash.encode()));
            callBuffer.flushBuffer();
        } catch (Exception e) {
            throw Exceptions.getIOException(e);
        } finally{
            try{
                callBuffer.close();
            }catch(Exception e){
                throw Exceptions.getIOException(e);
            }
        }
    }

    private String getConglomIdString() throws IOException {
        String conglom = conglomIdString;
        if(conglom==null){
            synchronized (this){
                conglom = conglomIdString;
                if(conglom==null){
                    try{
                    conglom = conglomIdString = fetchConglomId();
                    }catch(SQLException se){
                        throw Exceptions.getIOException(se);
                    } catch (StandardException e) {
                        throw Exceptions.getIOException(e);
                    }
                }
            }
        }
        return conglom;
    }

    private String fetchConglomId() throws StandardException,SQLException {
        EmbedConnection dbConn = (EmbedConnection)SpliceDriver.driver().getInternalConnection();
        LanguageConnectionContext lcc = dbConn.getLanguageConnection();
        DataDictionary dd = lcc.getDataDictionary();
        SchemaDescriptor systemSchemaDescriptor = dd.getSystemSchemaDescriptor();
        TableDescriptor td = dd.getTableDescriptor(tableName,systemSchemaDescriptor,lcc.getTransactionExecute());
        return Long.toString(td.getHeapConglomerateId());
    }

    protected static abstract class WriteableHash<T> implements DataHash<T> {
        protected T element;

        @Override public void setRow(T rowToEncode) { this.element = rowToEncode;	 }
        @Override public KeyHashDecoder getDecoder() { return null; }

        protected abstract void doEncode(MultiFieldEncoder encoder, T element);
    }

    protected static abstract class EntryWriteableHash<T> extends WriteableHash<T>{
        private EntryEncoder entryEncoder;

        @Override
        public final byte[] encode() throws StandardException, IOException {
            if(entryEncoder==null)
                entryEncoder = buildEncoder();

            MultiFieldEncoder fieldEncoder = entryEncoder.getEntryEncoder();
            fieldEncoder.reset();
            doEncode(fieldEncoder, element);
            return entryEncoder.encode();
        }

        protected abstract EntryEncoder buildEncoder();

        @Override
        public void close() throws IOException {
            if(entryEncoder!=null)
                entryEncoder.close();
        }
    }

    protected  static abstract class KeyWriteableHash<T> extends WriteableHash<T>{
        private MultiFieldEncoder entryEncoder;

        @Override
        public final byte[] encode() throws StandardException, IOException {
            if(entryEncoder==null)
                entryEncoder = MultiFieldEncoder.create(getNumFields());
            else
                entryEncoder.reset();

            doEncode(entryEncoder,element);
            return entryEncoder.build();
        }

        protected abstract int getNumFields();

        @Override
        public void close() throws IOException {

        }
    }
}
