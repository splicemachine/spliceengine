package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.index.PopulateIndexJob;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.job.JobFuture;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

/**
 * Operation to create an index in a Splice-efficient fashion
 *
 * @author Scott Fines
 * Created on: 3/7/13
 */
public class CreateIndexConstantOperationScott extends IndexConstantOperation implements ConstantAction {
    private static final long serialVersionUID = 1l;
    private static final Logger LOG = Logger.getLogger(CreateIndexConstantOperationScott.class);

    private String[] columnNames;
    private boolean[] ascending;
    private UUID tableId;
    private UUID conglomerateUUID;
    private boolean unique;
    private String indexType;
    private Properties properties;

    public CreateIndexConstantOperationScott(String schemaName,
                                String indexName, String tableName,
                                String[] columnNames,boolean[] isAscending,
                                UUID tableId, UUID conglomerateUUID,
                                boolean unique, String indexType, Properties properties) {
    	super(tableId, indexName, tableName, schemaName);
		
        this.columnNames = columnNames;
        ascending = isAscending;
        this.tableId = tableId;
        this.conglomerateUUID = conglomerateUUID;
        this.unique = unique;
        this.indexType = indexType;
        this.properties = properties;
    }

    public CreateIndexConstantOperationScott(ConglomerateDescriptor indexConglomDescriptor,
                                TableDescriptor mainTableDescriptor,
                                Properties properties) throws StandardException {
    	super(mainTableDescriptor.getUUID(), indexConglomDescriptor.getConglomerateName(), mainTableDescriptor.getName(), mainTableDescriptor.getSchemaName());
        this.conglomerateUUID = indexConglomDescriptor.getUUID();
        this.indexType = indexConglomDescriptor.getIndexDescriptor().indexType();
        this.properties = properties;

        //populate column names and ascending information
        this.columnNames = indexConglomDescriptor.getColumnNames();

        IndexRowGenerator irg = indexConglomDescriptor.getIndexDescriptor();
        this.unique = irg.isUnique();
        this.indexType = irg.indexType();
        this.ascending = irg.isAscending();

        /*
         * Sometimes the indexConglomDescriptor doesn't know the names of the columns
         * it's moving, so we have to populate it from the index column map and the
         * main table descriptor.
         */
        if(columnNames==null){
            int[] baseCols = irg.baseColumnPositions();
            columnNames = new String[baseCols.length];
            ColumnDescriptorList cdl = mainTableDescriptor.getColumnDescriptorList();
            for(int i=0;i<baseCols.length;i++){
                columnNames[i] = cdl.elementAt(baseCols[i]-1).getColumnName();
            }
        }
    }

    @Override
    public void executeConstantAction(final Activation activation) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();

        TableDescriptor td = getActiveTableDescriptor(activation, dd, lcc);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_CREATE_INDEX_NO_TABLE,
                    indexName, tableName);
        }
        if (td.getTableType() == TableDescriptor.SYSTEM_TABLE_TYPE) {
            throw StandardException.newException(SQLState.LANG_CREATE_SYSTEM_INDEX_ATTEMPTED,
                    indexName, tableName);
        }

        final int[] baseColumnPositions = getIndexToBaseTableMap(td);
        //set the properties of the index to the store
        Properties indexProperties = properties!=null?properties: new Properties();

        indexProperties.put("baseConglomerateId",Long.toString(td.getHeapConglomerateId()));
        indexProperties.put("nUniqueColumns",
                Integer.toString(unique? baseColumnPositions.length: baseColumnPositions.length+1));
        indexProperties.put("rowLocationColumn",
                Integer.toString(baseColumnPositions.length));
        indexProperties.put("nKeyFields",
                Integer.toString(baseColumnPositions.length+1));


        //build a template Row for the index
        ExecRow rowTemplate = activation.getExecutionFactory()
                .getValueRow(baseColumnPositions.length + 1);
        ColumnDescriptorList cdl = td.getColumnDescriptorList();
        int[] collationIds = new int[baseColumnPositions.length+1];
        int collationPos = 0;
        for(int colPos:baseColumnPositions){
            ColumnDescriptor cd = cdl.elementAt(colPos-1);
            rowTemplate.setColumn(collationPos+1,cd.getType().getNull());
            collationIds[collationPos] = cd.getType().getCollationType();
            collationPos++;
        }
        DataValueDescriptor locDesc = new HBaseRowLocation();
        rowTemplate.setColumn(baseColumnPositions.length+1,locDesc);
        collationIds[collationPos++] = locDesc.getTypeFormatId();

        //build a column ordering
        int numColOrdering = baseColumnPositions.length+1;
        ColumnOrdering[] order = new ColumnOrdering[numColOrdering];
        for(int i=0;i<numColOrdering;i++){
            order[i] = new IndexColumnOrder(i, i >= ascending.length || ascending[i]);
        }


        //create a new Conglomerate for this index
        final long indexConglomId = lcc.getTransactionExecute()
                .createConglomerate(indexType,
                        rowTemplate.getRowArray(),
                        order,
                        collationIds,
                        indexProperties,
                        TransactionController.IS_DEFAULT);

        //create a Conglomerate with the conglomId filled in and add it if we don't have one already
        IndexRowGenerator irg = new IndexRowGenerator(indexType,
                unique,baseColumnPositions,ascending,baseColumnPositions.length+1);
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName,lcc.getTransactionExecute(),true);
        ConglomerateDescriptor cgd = dd.getDataDescriptorGenerator()
                .newConglomerateDescriptor(indexConglomId, indexName,
                        true, irg,
                        false, conglomerateUUID,
                        td.getUUID(),
                        sd.getUUID());

        dd.addDescriptor(cgd,sd,DataDictionary.SYSCONGLOMERATES_CATALOG_NUM,false,lcc.getTransactionExecute());

        ConglomerateDescriptorList congloms = td.getConglomerateDescriptorList();
        congloms.add(cgd);
        conglomerateUUID = cgd.getUUID();

        /*
         * Now we have to build the index.
         *
         * We do this in mass parallel by calling SpliceIndexProtocol.buildIndex()
         */
        SpliceLogUtils.debug(LOG,"Building the initial index");
        final long tableConglomId = td.getHeapConglomerateId();
        final boolean isUnique = cgd.getIndexDescriptor().isUnique();
        final boolean uniqueWithDuplicateNulls = cgd.getIndexDescriptor().isUniqueWithDuplicateNulls();
        final String transactionId = getTransactionId(activation.getLanguageConnectionContext().getTransactionExecute());
        HTableInterface table = SpliceAccessManager.getHTable(tableConglomId);
        JobFuture future = null;
        try{
            boolean [] desc = new boolean[ascending.length];
            for(int i=0;i<ascending.length;i++){
                desc[i] = !ascending[i];
            }
            future = SpliceDriver.driver().getJobScheduler().submit(new PopulateIndexJob(table,
                                                                                         transactionId,
                                                                                         indexConglomId,
                                                                                         tableConglomId,
                                                                                         baseColumnPositions,
                                                                                         isUnique,
                                                                                         uniqueWithDuplicateNulls,
                                                                                         desc,
                                                                                         -1l,
                                                                                         -1l,
                                                                                         null, null, null));

            future.completeAll(null); //TODO -sf- add status hook
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        } finally {
            if(future!=null){
                try {
                    future.cleanup();
                } catch (ExecutionException e) {
                    throw Exceptions.parseException(e.getCause());
                }
            }
            try {
                table.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close HTable",e);
            }
        }
    }

    private TableDescriptor getActiveTableDescriptor(Activation activation, DataDictionary dd, LanguageConnectionContext lcc) throws StandardException {
        TableDescriptor td = activation.getDDLTableDescriptor();
        TransactionController tc = lcc.getTransactionExecute();
        if(td ==null){
            if(tableId!=null) 
                td = dd.getTableDescriptor(tableId);
             else
                td = dd.getTableDescriptor(tableName, dd.getSchemaDescriptor(schemaName,tc,true),tc);
        }
        return td;
    }

    private int[] getIndexToBaseTableMap(TableDescriptor td) throws StandardException {
        int[] indexPosMap = new int[columnNames.length];
        int pos=0;
        for(String columnName:columnNames) {
            ColumnDescriptor columnDescriptor = td.getColumnDescriptor(columnName);
            if(columnDescriptor==null)
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,columnName,tableName);
            indexPosMap[pos] = columnDescriptor.getPosition();
            pos++;
        }
        return indexPosMap;
    }


	/**
	 * Get the UUID for the conglomerate descriptor that was created
	 * (or re-used) by this constant action.
	 */
	public UUID getCreatedUUID() {
		return conglomerateUUID;
	}

}
