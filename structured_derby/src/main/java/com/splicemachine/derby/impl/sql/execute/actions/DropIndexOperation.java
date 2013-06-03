package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.Properties;

/**
 * Drops an index from a table.
 *
 * @author Scott Fines
 * Created on: 3/7/13
 */
public class DropIndexOperation implements ConstantAction {

    private final String fullIndexName;
    private final String indexName;
    private final String tableName;
    private final String schemaName;
    private final UUID tableId;
    private final long tableConglomerateId;

    public DropIndexOperation(String fullIndexName, String indexName,
                              String tableName, String schemaName,
                              UUID tableId, long tableConglomerateId) {
        this.fullIndexName = fullIndexName;
        this.indexName = indexName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.tableId = tableId;
        this.tableConglomerateId = tableConglomerateId;
    }

    @Override
    public void executeConstantAction(Activation activation) throws StandardException {

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        TableDescriptor td = dd.getTableDescriptor(tableId);
        if(td==null){
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,tableName);
        }

        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName,tc,true);


        ConglomerateDescriptor conglomerateDescriptor = dd.getConglomerateDescriptor(indexName,sd,true);
        if(conglomerateDescriptor==null){
            throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION,fullIndexName);
        }

        dropIndex(td,conglomerateDescriptor);
        dropConglomerate(conglomerateDescriptor,td,activation,lcc);
    }

    private void dropIndex(TableDescriptor td,
                           ConglomerateDescriptor conglomerateDescriptor) throws StandardException {
        final long tableConglomId = td.getHeapConglomerateId();
        final long indexConglomId = conglomerateDescriptor.getConglomerateNumber();

        //drop the index trigger from the main table
        HTableInterface mainTable = SpliceAccessManager.getHTable(tableConglomId);
        try {
            mainTable.coprocessorExec(SpliceIndexProtocol.class,
                    HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,
                    new Batch.Call<SpliceIndexProtocol, Void>() {
                        @Override
                        public Void call(SpliceIndexProtocol instance) throws IOException {
                            instance.dropIndex(indexConglomId,tableConglomId);
                            return null;
                        }
                    }) ;
        } catch (Throwable throwable) {
            throw Exceptions.parseException(throwable);
        }
    }

    private void dropConglomerate(ConglomerateDescriptor conglomDesc, TableDescriptor td,
                                  Activation activation, LanguageConnectionContext lcc)
            throws StandardException{
        Properties ixProps = new Properties();
        loadIndexProperties(lcc,conglomDesc,ixProps);

        ConglomerateDescriptor newBackingConglomCD = conglomDesc.drop(lcc,td);

        if(newBackingConglomCD==null) return;

        executeConglomReplacement(
                getConglomReplacementAction(newBackingConglomCD,td,ixProps),
                activation );
    }

    private void executeConglomReplacement(ConstantAction conglomReplacementAction,
                                           Activation activation) throws StandardException {
        conglomReplacementAction.executeConstantAction(activation);
    }

    private ConstantAction getConglomReplacementAction(ConglomerateDescriptor newBackingConglomCD,
                                             TableDescriptor td, Properties ixProps)
            throws StandardException {
        return new CreateIndexOperation(newBackingConglomCD,td,ixProps);
    }

    private void loadIndexProperties(LanguageConnectionContext lcc,
                                     ConglomerateDescriptor conglomDesc,
                                     Properties ixProps) throws StandardException {
        ConglomerateController cc = lcc.getTransactionExecute().openConglomerate(
                conglomDesc.getConglomerateNumber(),
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE);
        cc.getInternalTablePropertySet(ixProps);
        cc.close();
    }

}
