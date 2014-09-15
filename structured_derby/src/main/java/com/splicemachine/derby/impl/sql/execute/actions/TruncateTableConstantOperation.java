package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;

import java.util.Properties;

/**
 * Operation to truncate a specific table.
 *
 * @author Scott Fines
 * Date: 9/2/14
 */
public class TruncateTableConstantOperation extends AlterTableConstantOperation{

    /**
     * Make the AlterAction for an ALTER TABLE statement.
     *
     * @param sd                     descriptor for the table's schema.
     * @param tableName              Name of table.
     * @param tableId                UUID of table
     * @param tableConglomerateId    heap conglomerate number of table
     * @param lockGranularity        The lock granularity.
     * @param behavior               drop behavior for dropping column
     * @param sequential             If compress table/drop column,
*                               whether or not sequential
     * @param indexNameForStatistics Will name the index whose statistics
*                               will be updated/dropped. This param is looked at only if
     */
    public TruncateTableConstantOperation(SchemaDescriptor sd,
                                          String tableName,
                                          UUID tableId,
                                          long tableConglomerateId,
                                          char lockGranularity,
                                          int behavior,
                                          boolean sequential,
                                          String indexNameForStatistics) {
        super(sd,tableName,tableId,tableConglomerateId,null,null,lockGranularity,behavior,indexNameForStatistics);
    }


    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
        TransactionController userTxnController = activation.getTransactionController();
        SpliceTransactionManager child = (SpliceTransactionManager)userTxnController.startNestedUserTransaction(false,false);
        ((SpliceTransaction)child.getRawTransaction()).elevate(Long.toString(tableConglomerateId).getBytes());
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        lcc.pushNestedTransaction(child);
        try {
            //body is executing inside of a child transaction now
            doTruncate(activation);
        } catch(StandardException se){
            child.abort();
            throw se;
        }finally{
            lcc.popNestedTransaction();
        }
        child.commit();
    }

    private void doTruncate(Activation activation) throws StandardException{
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();

        TableDescriptor td = getTableDescriptor(dd);
        dd.getDependencyManager().invalidateFor(td, DependencyManager.TRUNCATE_TABLE,lcc);
        activation.setDDLTableDescriptor(td);

        adjustUDTDependencies(lcc,dd,td,null,false);
        truncateTable(td, activation);
    }

    /*
    * TRUNCATE TABLE  TABLENAME; (quickly removes all the rows from table and
    * it's correctponding indexes).
    * Truncate is implemented by dropping the existing conglomerates(heap,indexes) and recreating a
    * new ones  with the properties of dropped conglomerates. Currently Store
    * does not have support to truncate existing conglomerated until store
    * supports it , this is the only way to do it.
    * Error Cases: Truncate error cases same as other DDL's statements except
    * 1)Truncate is not allowed when the table is references by another table.
    * 2)Truncate is not allowed when there are enabled delete triggers on the table.
    * Note: Because conglomerate number is changed during recreate process all the statements will be
    * marked as invalide and they will get recompiled internally on their next
    * execution. This is okay because truncate makes the number of rows to zero
    * it may be good idea to recompile them becuase plans are likely to be
    * incorrect. Recompile is done internally by Derby, user does not have
    * any effect.
    */
    private void truncateTable(TableDescriptor td, Activation activation) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        TransactionController tc = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();
        ExecRow emptyHeapRow;
        long newHeapConglom;
        Properties properties = new Properties();

        //truncate table is not allowed if there are any tables referencing it.
        //except if it is self referencing.
        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
        for(int index = 0; index < cdl.size(); index++) {
            ConstraintDescriptor cd = cdl.elementAt(index);
            if (cd instanceof ReferencedKeyConstraintDescriptor) {
                ReferencedKeyConstraintDescriptor rfcd = (ReferencedKeyConstraintDescriptor) cd;
                if(rfcd.hasNonSelfReferencingFK(ConstraintDescriptor.ENABLED)) {
                    throw StandardException.newException(SQLState.LANG_NO_TRUNCATE_ON_FK_REFERENCE_TABLE,td.getName());
                }
            }
        }

        //truncate is not allowed when there are enabled DELETE triggers
        GenericDescriptorList tdl = dd.getTriggerDescriptors(td);
        for (Object aTdl : tdl) {
            TriggerDescriptor trd = (TriggerDescriptor) aTdl;
            if (trd.listensForEvent(TriggerDescriptor.TRIGGER_EVENT_DELETE) && trd.isEnabled()) {
                throw StandardException.newException(SQLState.LANG_NO_TRUNCATE_ON_ENABLED_DELETE_TRIGGERS, td.getName(), trd.getName());
            }
        }

        //gather information from the existing conglomerate to create new one.
        emptyHeapRow = td.getEmptyExecRow();
        ConglomerateController compressHeapCC = tc.openConglomerate(
                td.getHeapConglomerateId(),
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE);

        // Get column ordering for new conglomerate
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager)tc).findConglomerate(td.getHeapConglomerateId());
        int[] columnOrder = conglomerate.getColumnOrdering();
        ColumnOrdering[] columnOrdering = null;
        if (columnOrder != null) {
            columnOrdering = new ColumnOrdering[columnOrder.length];
            for (int i = 0; i < columnOrder.length; i++) {
                columnOrdering[i] = new IndexColumnOrder(columnOrder[0]);
            }
        }

        // Get the properties on the old heap
        compressHeapCC.getInternalTablePropertySet(properties);
        compressHeapCC.close();

        //create new conglomerate
        newHeapConglom =
                tc.createConglomerate(
                        "heap",
                        emptyHeapRow.getRowArray(),
                        columnOrdering, //column sort order - not required for heap
                        td.getColumnCollationIds(),
                        properties,
                        TransactionController.IS_DEFAULT);

		    /* Set up index info to perform truncate on them*/
        int numIndexes = getAffectedIndexes(td);

		    /*
		    ** Inform the data dictionary that we are about to write to it.
		    ** There are several calls to data dictionary "get" methods here
		    ** that might be done in "read" mode in the data dictionary, but
		    ** it seemed safer to do this whole operation in "write" mode.
		    **
		    ** We tell the data dictionary we're done writing at the end of
		    ** the transaction.
		     */
        dd.startWriting(lcc);

        // truncate  all indexes
        if(numIndexes > 0) {
            long[] newIndexCongloms = new long[numIndexes];
            for (int index = 0; index < numIndexes; index++) {
                updateIndex(newHeapConglom, activation, tc, td, index, newIndexCongloms);
            }
        }

        // Update the DataDictionary
        // Get the ConglomerateDescriptor for the heap
        long oldHeapConglom = td.getHeapConglomerateId();
        ConglomerateDescriptor cd = td.getConglomerateDescriptor(oldHeapConglom);

        // Update sys.sysconglomerates with new conglomerate #
        dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);

        // Now that the updated information is available in the system tables,
        // we should invalidate all statements that use the old conglomerates
        dd.getDependencyManager().invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);

        // Drop the old conglomerate
        cleanUp();
    }

    @Override
    protected void doIndexUpdate(DataDictionary dd,
                                 TableDescriptor td,
                                 TransactionController tc,
                                 int index,
                                 long[] newIndexCongloms,
                                 ConglomerateDescriptor cd,
                                 Properties properties,
                                 boolean statisticsExist,
                                 DataValueDescriptor[] rowArray,
                                 ColumnOrdering[] columnOrder,
                                 int[] collationIds) throws StandardException {
        newIndexCongloms[index] =
                tc.createConglomerate(
                        "BTREE",
                        rowArray,
                        columnOrder,
                        collationIds,
                        properties,
                        TransactionController.IS_DEFAULT);


        //on truncate drop the statistics because we know for sure
        //rowscount is zero and existing statistic will be invalid.
        if (td.statisticsExist(cd))
            dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
    }

    @Override
    protected int getIndexedColumnSize(TableDescriptor td) {
        return td.getNumberOfColumns()+1;
    }

    @Override
    public String toString() {
        return "TRUNCATE TABLE " + tableName;
    }
}
