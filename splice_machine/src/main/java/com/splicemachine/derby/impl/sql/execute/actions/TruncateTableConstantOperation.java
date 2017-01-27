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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.execute.TriggerEventDML;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.job.fk.FkJobSubmitter;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.protobuf.ProtoUtil;

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
     * @param lockGranularity        The lock granularity.
     * @param behavior               drop behavior for dropping column
     * @param indexNameForStatistics Will name the index whose statistics
     */
    public TruncateTableConstantOperation(SchemaDescriptor sd,
                                          String tableName,
                                          UUID tableId,
                                          char lockGranularity,
                                          int behavior,
                                          String indexNameForStatistics) {
        super(sd,tableName,tableId,null,null,behavior,indexNameForStatistics);
    }


    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
        doTruncate(activation);
    }

    private void doTruncate(Activation activation) throws StandardException{
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        //tell the data dictionary that we are going to be writing some data
        dd.startWriting(lcc);

        TableDescriptor td = getTableDescriptor(lcc);
        dd.getDependencyManager().invalidateFor(td, DependencyManager.TRUNCATE_TABLE,lcc);
        activation.setDDLTableDescriptor(td);

        adjustUDTDependencies(activation,null,false);
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
            if (trd.listensForEvent(TriggerEventDML.DELETE) && trd.isEnabled()) {
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
                columnOrdering[i] = new IndexColumnOrder(columnOrder[i]);
            }
        }

        // Get the properties on the old heap
        compressHeapCC.getInternalTablePropertySet(properties);
        compressHeapCC.close();

        //create new conglomerate
        newHeapConglom =
                tc.createConglomerate(
                        td.isExternal(),
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

        // If the table has foreign key, drop the old foreign key write handler
        ConstraintDescriptorList constraintDescriptors = td.getConstraintDescriptorList();
        for(int i = 0; i < constraintDescriptors.size(); ++i) {
            ConstraintDescriptor conDesc = constraintDescriptors.get(i);
            if (conDesc instanceof ForeignKeyConstraintDescriptor) {
                ForeignKeyConstraintDescriptor d = (ForeignKeyConstraintDescriptor) conDesc;
                ReferencedKeyConstraintDescriptor referencedConstraint = d.getReferencedConstraint();
                new FkJobSubmitter(dd, (SpliceTransactionManager) tc, referencedConstraint, conDesc, DDLChangeType.DROP_FOREIGN_KEY,lcc).submit();
            }
        }

        // truncate  all indexes
        if(numIndexes > 0) {
            long[] newIndexCongloms = new long[numIndexes];
            for (int index = 0; index < numIndexes; index++) {
                updateIndex(newHeapConglom, activation, tc, td, index, newIndexCongloms);
            }
        }

        // If the table has foreign key, create a new foreign key write handler
        for(int i = 0; i < constraintDescriptors.size(); ++i) {
            ConstraintDescriptor conDesc = constraintDescriptors.get(i);
            if (conDesc instanceof ForeignKeyConstraintDescriptor) {
                ForeignKeyConstraintDescriptor d = (ForeignKeyConstraintDescriptor) conDesc;
                ReferencedKeyConstraintDescriptor referencedConstraint = d.getReferencedConstraint();
                new FkJobSubmitter(dd, (SpliceTransactionManager) tc, referencedConstraint, conDesc, DDLChangeType.ADD_FOREIGN_KEY,lcc).submit();
            }
        }
        // Invalidate cache
        DDLMessage.DDLChange change = ProtoUtil.createTruncateTable(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), (BasicUUID) tableId);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(change));

        // Update the DataDictionary
        // Get the ConglomerateDescriptor for the heap
        long oldHeapConglom = td.getHeapConglomerateId();
        ConglomerateDescriptor cd = td.getConglomerateDescriptor(oldHeapConglom);

        // Update sys.sysconglomerates with new conglomerate #
        dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);

        // Now that the updated information is available in the system tables,
        // we should invalidate all statements that use the old conglomerates
        dd.getDependencyManager().invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);

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
                        td.isExternal(),
                        "BTREE",
                        rowArray,
                        columnOrder,
                        collationIds,
                        properties,
                        TransactionController.IS_DEFAULT);


    }

    @Override
    public String toString() {
        return "TRUNCATE TABLE " + tableName;
    }

    public String getScopeName() {
        return String.format("Truncate Table %s", tableName);
    }
}
