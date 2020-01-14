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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryCache;
import com.splicemachine.db.impl.sql.catalog.TableKey;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.pin.DistributedIsCachedJob;
import com.splicemachine.derby.impl.sql.execute.pin.GetIsCachedResult;
import com.splicemachine.derby.impl.sql.execute.pin.RemoteDropPinJob;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;


/**
 * This class describes actions that are ALWAYS performed for a DROP TABLE Statement at Execution time.
 */
public class DropTableConstantOperation extends DDLSingleTableConstantOperation {
    private final long conglomerateNumber;
    private final String fullTableName;
    private final SchemaDescriptor sd;
    private final boolean cascade;

    /**
     * Make the ConstantAction for a DROP TABLE statement.
     *
     * @param fullTableName      Fully qualified table name
     * @param tableName          Table name.
     * @param sd                 Schema that table lives in.
     * @param conglomerateNumber Conglomerate number for heap
     * @param tableId            UUID for table
     * @param behavior           drop behavior: RESTRICT, CASCADE or default
     */
    public DropTableConstantOperation(String fullTableName, String tableName, SchemaDescriptor sd,
                                      long conglomerateNumber, UUID tableId, int behavior) {
        super(tableId);
        this.fullTableName = fullTableName;
        this.sd = sd;
        this.conglomerateNumber = conglomerateNumber;
        this.cascade = (behavior == StatementType.DROP_CASCADE);
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
    }

    @Override
    public String toString() {
        return "DROP TABLE " + fullTableName;
    }

    /**
     * This is the guts of the Execution-time logic for DROP TABLE.
     */
    @Override
    public void executeConstantAction(Activation activation) throws StandardException {

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();
      /*
       * Inform the data dictionary that we are about to write to it.
       */
        dd.startWriting(lcc);

        /* Get the table descriptor. */
        TableDescriptor td = dd.getTableDescriptor(tableId);
        activation.setDDLTableDescriptor(td);

        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
        }

        long heapId = td.getHeapConglomerateId();

        try {
            /* Drop the triggers */
            for (Object aTdl : dd.getTriggerDescriptors(td)) {
                TriggerDescriptor trd = (TriggerDescriptor) aTdl;
                trd.drop(lcc);
            }

            /* Drop all column defaults */
            for (ColumnDescriptor cd : td.getColumnDescriptorList()) {
                // If column has a default we drop the default and any dependencies
                if (cd.getDefaultInfo() != null) {
                    DefaultDescriptor defaultDesc = cd.getDefaultDescriptor(dd);
                    dm.clearDependencies(lcc, defaultDesc);
                }
            }

            /* Drop all table and column permission descriptors */
            dd.dropAllTableAndColPermDescriptors(tableId, tc);

            /* Drop the constraints */
            dropAllConstraintDescriptors(td, activation);

            /* Drop the columns */
            dd.dropAllColumnDescriptors(tableId, tc);

            /*
             * Drop all the conglomerates.  Drop the heap last, because the
             * store needs it for locking the indexes when they are dropped.
             */
            ConglomerateDescriptor[] cds = td.getConglomerateDescriptors();
            long[] dropped = new long[cds.length - 1];
            int numDropped = 0;
            for (ConglomerateDescriptor cd : cds) {

                /* Remove Statistics*/
                    dd.deletePartitionStatistics(cd.getConglomerateNumber(), tc);

                /*
                 * if it's for an index, since similar indexes share one conglomerate, we only drop the conglomerate once
                 */
                if (cd.getConglomerateNumber() != heapId) {
                    long thisConglom = cd.getConglomerateNumber();

                    int i;
                    for (i = 0; i < numDropped; i++) {
                        if (dropped[i] == thisConglom) {
                            break;
                        }
                    }
                    if (i == numDropped) {
                        // not dropped
                        dropped[numDropped++] = thisConglom;
                        tc.dropConglomerate(thisConglom);
                    }
                }

            }

            /* Invalidate dependencies remotely. */
            TxnView activeTransaction = ((SpliceTransactionManager) tc).getActiveStateTxn();
            DDLChange ddlChange = ProtoUtil.createDropTable(activeTransaction.getTxnId(), (BasicUUID) this.tableId);
            // Run locally first to capture any errors.
            dm.invalidateFor(td, DependencyManager.DROP_TABLE, lcc);
            // Run Remotely
            tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

            // The table itself can depend on the user defined types of its columns. Drop all of those dependencies now.
            adjustUDTDependencies(activation, null, true);

            // remove from LCC
            lcc.dropDeclaredGlobalTempTable(td);

            /* Drop the table */
            dd.dropTableDescriptor(td, sd, tc);

            /* Drop the conglomerate descriptors */
            dd.dropAllConglomerateDescriptors(td, tc);

            /* Drop the store element at last, to prevent dangling reference for open cursor, beetle 4393. */
            tc.dropConglomerate(heapId);

            /* is the table pinned ? , if yes we need to drop it */
            try {
                GetIsCachedResult isCachedResult = EngineDriver.driver().getOlapClient().execute(new DistributedIsCachedJob(heapId));
                if(isCachedResult.isCached()) {
                    EngineDriver.driver().getOlapClient().execute(new RemoteDropPinJob(heapId));
                }
            } catch (Exception e) {
                throw StandardException.plainWrapException(e);
            }

        } catch (Exception e) {
            // If dropping table fails, it could happen that the table object in cache has been modified.
            // Invalidate the table in cache.
            DataDictionaryCache cache = dd.getDataDictionaryCache();
            TableKey tableKey = new TableKey(td.getSchemaDescriptor().getUUID(), td.getName());
            cache.nameTdCacheRemove(tableKey);
            cache.oidTdCacheRemove(td.getUUID());
            throw e;
        }
    }

    private void dropAllConstraintDescriptors(TableDescriptor td, Activation activation) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);

        /*
         * First go, don't drop unique or primary keys.
         * This will ensure that self-referential constraints
         * will work ok, even if not cascading.
         */
        ConstraintDescriptor cd;
        for (int index = 0; index < cdl.size(); ) {
            /* The current element will be deleted underneath
             * the loop, so we only increment the counter when
             * skipping an element. (HACK!) */
            cd = cdl.elementAt(index);
            if (cd instanceof ReferencedKeyConstraintDescriptor) {
                index++;
                continue;
            }

            dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
            dropConstraint(cd, td, activation, lcc, true);
        }

        /*
         * Referenced keys (unique or pk) constraints only
         */
        while (!cdl.isEmpty()) {
            /* The current element will be deleted underneath the loop. (HACK!) */
            cd = cdl.elementAt(0);
            if (SanityManager.DEBUG) {
                if (!(cd instanceof ReferencedKeyConstraintDescriptor)) {
                    SanityManager.THROWASSERT("Constraint descriptor not an instance of " +
                            "ReferencedKeyConstraintDescriptor as expected.  Is a " + cd.getClass().getName());
                }
            }

            /*
            * Drop the referenced constraint (after we got
            * the primary keys) now.  Do this prior to
            * dropping the referenced keys to avoid performing
            * a lot of extra work updating the referencedcount
            * field of sys.sysconstraints.
            *
            * Pass in false to dropConstraintsAndIndex so it
            * doesn't clear dependencies, we'll do that ourselves.
            */
            dropConstraint(cd, td, activation, lcc, false);

            /*
             * If we are going to cascade, get all the referencing foreign keys and zap them first.
             */
            if (cascade) {
                /*
                 * Go to the system tables to get the foreign keys to be safe
                 */
                ConstraintDescriptorList fkcdl = dd.getForeignKeys(cd.getUUID());

                /*
                 * For each FK that references this key, drop it.
                 */
                for (int inner = 0; inner < fkcdl.size(); inner++) {
                    ConstraintDescriptor fkcd = fkcdl.elementAt(inner);
                    dm.invalidateFor(fkcd, DependencyManager.DROP_CONSTRAINT, lcc);
                    dropConstraint(fkcd, td, activation, lcc, true);
                    activation.addWarning(
                            StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
                                    fkcd.getConstraintName(),
                                    fkcd.getTableDescriptor().getName()));
                }
            }

            /*
             * Now that we got rid of the fks (if we were cascading), it is
             * ok to do an invalidate for.
             */
            dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
            dm.clearDependencies(lcc, cd);
        }
    }

    public String getScopeName() {
        return String.format("Drop Table %s", fullTableName);
    }

}
