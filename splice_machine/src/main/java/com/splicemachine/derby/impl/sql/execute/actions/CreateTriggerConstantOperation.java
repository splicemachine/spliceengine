/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.execute.TriggerEventDML;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

import java.sql.Timestamp;

/**
 * This class  describes actions that are ALWAYS performed for a
 * CREATE TRIGGER Statement at Execution time.
 */
public class CreateTriggerConstantOperation extends DDLSingleTableConstantOperation {

    private static final Logger LOG = Logger.getLogger(CreateTriggerConstantOperation.class);

    private final String triggerName;
    private final String triggerSchemaName;
    private final TriggerEventDML eventMask;
    private final boolean isBefore;
    private final boolean isRow;
    private final boolean isEnabled;
    private final boolean referencingOld;
    private final boolean referencingNew;
    private       UUID   whenSPSId;
    private final String whenText;
    private final String actionText;
    private final String originalWhenText;
    private final String originalActionText;
    private final String oldReferencingName;
    private final String newReferencingName;
    private final Timestamp creationTimestamp;
    private final int[] referencedCols;
    private final int[] referencedColsInTriggerAction;

    private UUID spsCompSchemaId;
    private TableDescriptor triggerTable;        // null after readExternal
    private UUID triggerTableId;        // set in readExternal
    private UUID actionSPSId;

    // CONSTRUCTORS

    /**
     * Make the ConstantAction for a CREATE TRIGGER statement.
     *
     * @param triggerSchemaName             name for the schema that trigger lives in.
     * @param triggerName                   Name of trigger
     * @param eventMask                     TriggerDescriptor.TRIGGER_EVENT_XXXX
     * @param isBefore                      is this a before (as opposed to after) trigger
     * @param isRow                         is this a row trigger or statement trigger
     * @param isEnabled                     is this trigger enabled or disabled
     * @param triggerTable                  the table upon which this trigger is defined
     * @param whenSPSId                     the sps id for the when clause (may be null)
     * @param whenText                      the text of the when clause (may be null)
     * @param actionSPSId                   the spsid for the trigger action (may be null)
     * @param actionText                    the text of the trigger action
     * @param spsCompSchemaId               the compilation schema for the action and when
     *                                      spses.   If null, will be set to the current default
     *                                      schema
     * @param creationTimestamp             when was this trigger created?  if null, will be
     *                                      set to the time that executeConstantAction() is invoked
     * @param referencedCols                what columns does this trigger reference (may be null)
     * @param referencedColsInTriggerAction what columns does the trigger
     *                                      action reference through old/new transition variables
     *                                      (may be null)
     * @param originalActionText            The original user text of the trigger action
     * @param referencingOld                whether or not OLD appears in REFERENCING clause
     * @param referencingNew                whether or not NEW appears in REFERENCING clause
     * @param oldReferencingName            old referencing table name, if any, that appears in REFERENCING clause
     * @param newReferencingName            new referencing table name, if any, that appears in REFERENCING clause
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public CreateTriggerConstantOperation(
            String triggerSchemaName,
            String triggerName,
            TriggerEventDML eventMask,
            boolean isBefore,
            boolean isRow,
            boolean isEnabled,
            TableDescriptor triggerTable,
            UUID whenSPSId,
            String whenText,
            UUID actionSPSId,
            String actionText,
            UUID spsCompSchemaId,
            Timestamp creationTimestamp,
            int[] referencedCols,
            int[] referencedColsInTriggerAction,
            String originalWhenText,
            String originalActionText,
            boolean referencingOld,
            boolean referencingNew,
            String oldReferencingName,
            String newReferencingName) {
        super(triggerTable.getUUID());
        SpliceLogUtils.trace(LOG, "CreateTriggerConstantOperation called for trigger %s", triggerName);
        this.triggerName = triggerName;
        this.triggerSchemaName = triggerSchemaName;
        this.triggerTable = triggerTable;
        this.eventMask = eventMask;
        this.isBefore = isBefore;
        this.isRow = isRow;
        this.isEnabled = isEnabled;
        this.whenSPSId = whenSPSId;
        this.whenText = whenText;
        this.actionSPSId = actionSPSId;
        this.actionText = actionText;
        this.spsCompSchemaId = spsCompSchemaId;
        this.creationTimestamp = creationTimestamp;
        this.referencedCols = referencedCols;
        this.referencedColsInTriggerAction = referencedColsInTriggerAction;
        this.originalActionText = originalActionText;
        this.originalWhenText = originalWhenText;
        this.referencingOld = referencingOld;
        this.referencingNew = referencingNew;
        this.oldReferencingName = oldReferencingName;
        this.newReferencingName = newReferencingName;
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(triggerSchemaName != null, "triggerSchemaName sd is null");
            SanityManager.ASSERT(triggerName != null, "trigger name is null");
            SanityManager.ASSERT(actionText != null, "actionText is null");
        }
    }

    /**
     * This is the guts of the Execution-time logic for CREATE TRIGGER.
     *
     * @throws StandardException Thrown on failure
     */
    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction for activation %s", activation);
        SPSDescriptor whenspsd = null;
        SPSDescriptor actionspsd;

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();

        /*
        ** Indicate that we are about to modify the data dictionary.
        ** 
        ** We tell the data dictionary we're done writing at the end of
        ** the transaction.
        */
        dd.startWriting(lcc);

        SchemaDescriptor triggerSd = getSchemaDescriptorForCreate(dd, activation, triggerSchemaName);

        if (spsCompSchemaId == null) {
            SchemaDescriptor def = lcc.getDefaultSchema();
            if (def.getUUID() == null) {
                // Descriptor for default schema is stale,
                // look it up in the dictionary
                def = dd.getSchemaDescriptor(def.getDescriptorName(), tc,
                        false);
            }

            /* 
            ** It is possible for spsCompSchemaId to be null.  For instance, 
            ** the current schema may not have been physically created yet but 
            ** it exists "virtually".  In this case, its UUID will have the 
            ** value of null meaning that it is not persistent.  e.g.:   
            **
            ** CONNECT 'db;create=true' user 'ernie';
            ** CREATE TABLE bert.t1 (i INT);
            ** CREATE TRIGGER bert.tr1 AFTER INSERT ON bert.t1 
            **    FOR EACH STATEMENT MODE DB2SQL 
            **    SELECT * FROM SYS.SYSTABLES;
            **
            ** Note that in the above case, the trigger action statement have a 
            ** null compilation schema.  A compilation schema with null value 
            ** indicates that the trigger action statement text does not have 
            ** any dependencies with the CURRENT SCHEMA.  This means:
            **
            ** o  It is safe to compile this statement in any schema since 
            **    there is no dependency with the CURRENT SCHEMA. i.e.: All 
            **    relevent identifiers are qualified with a specific schema.
            **
            ** o  The statement cache mechanism can utilize this piece of 
            **    information to enable better statement plan sharing across 
            **    connections in different schemas; thus, avoiding unnecessary 
            **    statement compilation.
            */
            if (def != null)
                spsCompSchemaId = def.getUUID();
        }

        String tabName;
        if (triggerTable != null) {
            triggerTableId = triggerTable.getUUID();
            tabName = triggerTable.getName();
        } else
            tabName = "with UUID " + triggerTableId;

        /* We need to get table descriptor again.  We simply can't trust the
         * one we got at compile time, the lock on system table was released
         * when compile was done, and the table might well have been dropped.
         */
        triggerTable = dd.getTableDescriptor(triggerTableId);
        if (triggerTable!=null && triggerTable.getTableType()==TableDescriptor.EXTERNAL_TYPE) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_NO_TRIGGERS,triggerTable.getName());
        }


        if (triggerTable == null) {
            throw StandardException.newException(
                    SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
                    tabName);
        }
        /* Lock the table for DDL.  Otherwise during our execution, the table
         * might be changed, even dropped.  Beetle 4269
         */
        triggerTable = dd.getTableDescriptor(triggerTableId);
        if (triggerTable == null) {
            throw StandardException.newException(
                    SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
                    tabName);
        }

        /*
        ** Send an invalidate on the table from which
        ** the triggering event emanates.  This it
        ** to make sure that DML statements on this table
        ** will be recompiled.  Do this before we create
        ** our trigger spses lest we invalidate them just
        ** after creating them.
        */
        dm.invalidateFor(triggerTable, DependencyManager.CREATE_TRIGGER, lcc);

        DDLMessage.DDLChange ddlChange = ProtoUtil.createTrigger(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), (BasicUUID) this.tableId);
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

        /*
        ** Lets get our trigger id up front, we'll use it when
         ** we create our spses.
        */
        UUID tmpTriggerId = dd.getUUIDFactory().createUUID();

        actionSPSId = (actionSPSId == null) ?
                dd.getUUIDFactory().createUUID() : actionSPSId;

        if (whenSPSId == null && whenText != null) {
            whenSPSId = dd.getUUIDFactory().createUUID();
        }

        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        /*
        ** Create the trigger descriptor first so the trigger action
        ** compilation can pick up the relevant trigger especially in 
        ** the case of self triggering.
        */
        TriggerDescriptor triggerd =
                ddg.newTriggerDescriptor(
                        triggerSd,
                        tmpTriggerId,
                        triggerName,
                        eventMask,
                        isBefore,
                        isRow,
                        isEnabled,
                        triggerTable,
                        whenSPSId,
                        actionSPSId,
                        creationTimestamp == null ? new Timestamp(System.currentTimeMillis()) : creationTimestamp,
                        referencedCols,
                        referencedColsInTriggerAction,
                        originalActionText,
                        referencingOld,
                        referencingNew,
                        oldReferencingName,
                        newReferencingName,
                        originalWhenText);


        dd.addDescriptor(triggerd, triggerSd,
                DataDictionary.SYSTRIGGERS_CATALOG_NUM, false,
                tc, false);


        /*    
        ** If we have a WHEN action we create it now.
        */
        if (whenText != null) {
            // The WHEN clause is just a search condition and not a full
            // SQL statement. Turn in into a VALUES statement.
            String whenValuesStmt = "VALUES ( " + whenText + " )";
            whenspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
                    whenSPSId, spsCompSchemaId, whenValuesStmt, true, triggerTable);
        }

        /*
        ** Create the trigger action
        */
        actionspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
                actionSPSId, spsCompSchemaId, actionText, false, triggerTable);
        
        /*
        ** Make underlying spses dependent on the trigger.
        */
        if (whenspsd != null) {
            dm.addDependency(triggerd, whenspsd, lcc.getContextManager());
        }
        dm.addDependency(triggerd, actionspsd, lcc.getContextManager());
        dm.addDependency(triggerd, triggerTable, lcc.getContextManager());
        //store trigger's dependency on various privileges in the dependency system
        storeViewTriggerDependenciesOnPrivileges(activation, triggerd);
    }


    /*
    ** Create an sps that is used by the trigger.
    */
    private SPSDescriptor createSPS(
            LanguageConnectionContext lcc,
            DataDescriptorGenerator ddg,
            DataDictionary dd,
            TransactionController tc,
            UUID triggerId,
            SchemaDescriptor sd,
            UUID spsId,
            UUID compSchemaId,
            String text,
            boolean isWhen,
            TableDescriptor triggerTable) throws StandardException {

        SpliceLogUtils.trace(LOG, "createSPS with text {%s} on table {%s}", text, triggerTable);

        if (text == null)
            return null;

        /*
        ** Note: the format of this string is very important.
        ** Dont change it arbitrarily -- see sps code.
        */
        String spsName = "TRIGGER" +
                (isWhen ? "WHEN_" : "ACTN_") +
                triggerId + "_" + triggerTable.getUUID().toString();

        SPSDescriptor spsd = new SPSDescriptor(dd, spsName,
                (spsId == null) ? dd.getUUIDFactory().createUUID() : spsId, sd.getUUID(),
                compSchemaId == null ? lcc.getDefaultSchema().getUUID() : compSchemaId,
                SPSDescriptor.SPS_TYPE_TRIGGER,
                true,                // it is valid
                text,                // the text
                true);    // no defaults

        /*
        ** Prepared the stored prepared statement
        ** and release the activation class -- we
        ** know we aren't going to execute statement
        ** after create it, so for now we are finished.
        */
        spsd.prepareAndRelease(lcc, triggerTable);
        dd.addSPSDescriptor(spsd, tc);

        return spsd;
    }

    @Override
    public String toString() {
        return constructToString("CREATE TRIGGER ", triggerName);
    }
}

