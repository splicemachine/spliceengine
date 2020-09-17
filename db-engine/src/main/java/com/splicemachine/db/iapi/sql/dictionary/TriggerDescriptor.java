/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Parser;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import com.splicemachine.db.impl.sql.execute.TriggerEvent;
import com.splicemachine.db.impl.sql.execute.TriggerEventDML;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * A trigger.
 * <p/>
 * We are dependent on TableDescriptors, SPSDescriptors (for our WHEN clause and our action).  Note that we don't
 * strictly need to be dependent on out SPSes because we could just disallow anyone from dropping an sps of type 'T',
 * but to keep dependencies uniform, we'll do be dependent.
 * <p/>
 * We are a provider for DML (PreparedStatements or SPSes)
 */
public class TriggerDescriptor extends TupleDescriptor implements UniqueSQLObjectDescriptor, Provider, Dependent, Formatable {

    // field that we want users to be able to know about
    public static final int SYSTRIGGERS_STATE_FIELD = 8;

    private UUID id;
    private String name;
    private String oldReferencingName;
    private String newReferencingName;
    List<String> triggerDefinitionList;
    private SchemaDescriptor sd;
    private TriggerEventDML triggerDML;
    private boolean isBefore;
    private boolean isRow;
    private boolean referencingOld;
    private boolean referencingNew;
    private TableDescriptor td;
    List<UUID> actionSPSIdList;
    private UUID whenSPSId;
    private boolean isEnabled;
    private int[] referencedCols;
    private int[] referencedColsInTriggerAction;
    private Timestamp creationTimestamp;
    private UUID triggerSchemaId;
    private UUID triggerTableId;
    private String whenClauseText;
    int numBaseTableColumns;
    protected int version;


    /**
     * Default constructor for formatable
     */
    public TriggerDescriptor() {
    }

    /**
     * Constructor.  Used when creating a trigger from SYS.SYSTRIGGERS
     *
     * @param dataDictionary                the data dictionary
     * @param sd                            the schema descriptor for this trigger
     * @param id                            the trigger id
     * @param name                          the trigger name
     * @param eventMask                     TriggerDescriptor.TRIGGER_EVENT_XXXX
     * @param isBefore                      is this a before (as opposed to after) trigger
     * @param isRow                         is this a row trigger or statement trigger
     * @param isEnabled                     is this trigger enabled or disabled
     * @param td                            the table upon which this trigger is defined
     * @param whenSPSId                     the sps id for the when clause (may be null)
     * @param actionSPSIdList               the sps ids for the trigger actions (may be null)
     * @param creationTimestamp             when was this trigger created?
     * @param referencedCols                what columns does this trigger reference (may be null)
     * @param referencedColsInTriggerAction what columns does the trigger
     *                                      action reference through old/new transition variables (may be null)
     * @param triggerDefinitionList         The original user texts of the trigger actions
     * @param referencingOld                whether or not OLD appears in REFERENCING clause
     * @param referencingNew                whether or not NEW appears in REFERENCING clause
     * @param oldReferencingName            old referencing table name, if any, that appears in REFERCING clause
     * @param newReferencingName            new referencing table name, if any, that appears in REFERCING clause
     * @param whenClauseText                the SQL text of the WHEN clause, or {@code null}
     *                                      if there is no WHEN clause
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "DB-9292")
    public TriggerDescriptor(
            DataDictionary dataDictionary,
            SchemaDescriptor sd,
            UUID id,
            String name,
            TriggerEventDML eventMask,
            boolean isBefore,
            boolean isRow,
            boolean isEnabled,
            TableDescriptor td,
            UUID whenSPSId,
            List<UUID> actionSPSIdList,
            Timestamp creationTimestamp,
            int[] referencedCols,
            int[] referencedColsInTriggerAction,
            List<String> triggerDefinitionList,
            boolean referencingOld,
            boolean referencingNew,
            String oldReferencingName,
            String newReferencingName,
            String whenClauseText) {
        super(dataDictionary);
        this.id = id;
        this.sd = sd;
        this.name = name;
        this.triggerDML = eventMask;
        this.isBefore = isBefore;
        this.isRow = isRow;
        this.td = td;
        this.actionSPSIdList = actionSPSIdList;
        this.whenSPSId = whenSPSId;
        this.isEnabled = isEnabled;
        this.referencedCols = referencedCols;
        this.referencedColsInTriggerAction = referencedColsInTriggerAction;
        this.creationTimestamp = creationTimestamp;
        this.triggerDefinitionList = triggerDefinitionList;
        this.referencingOld = referencingOld;
        this.referencingNew = referencingNew;
        this.oldReferencingName = oldReferencingName;
        this.newReferencingName = newReferencingName;
        this.triggerSchemaId = sd.getUUID();
        this.triggerTableId = td.getUUID();
        this.whenClauseText = whenClauseText;
        this.numBaseTableColumns = td.getNumberOfColumns();
        this.version = 1;
    }
    
    /**
     * Get the trigger UUID
     */
    @Override
    public UUID getUUID() {
        return id;
    }

    /**
     * Get the trigger name
     */
    @Override
    public String getName() {
        return name;
    }

    public UUID getTableId() {
        return triggerTableId;
    }

    /**
     * Get the triggers schema descriptor
     *
     * @return the schema descriptor
     */
    @Override
    public SchemaDescriptor getSchemaDescriptor() throws StandardException {
        if (sd == null) {
            sd = getDataDictionary().getSchemaDescriptor(triggerSchemaId, null);
        }
        return sd;
    }

    /**
     * Indicate whether this trigger listens for this type of event.
     */
    public boolean listensForEvent(TriggerEventDML event) {
        return triggerDML == event;
    }

    /**
     * Get the trigger event mask.  Currently, a trigger may only listen for a single event, though it may
     * OR multiple events in the future.
     */
    public TriggerEventDML getTriggerEventDML() {
        return triggerDML;
    }

    /**
     * Get the time that this trigger was created.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9292")
    public Timestamp getCreationTimestamp() {
        return creationTimestamp;
    }

    /**
     * Is this a before trigger
     *
     * @return true if it is a before trigger
     */
    public boolean isBeforeTrigger() {
        return isBefore;
    }

    /**
     * Is this a row trigger
     *
     * @return true if it is a before trigger
     */
    public boolean isRowTrigger() {
        return isRow;
    }

    /**
     * This method only makes sense as long as we support only one DML type per trigger.
     */
    public TriggerEvent getTriggerEvent() {
        for (TriggerEvent event : TriggerEvent.values()) {
            if (event.isBefore() == this.isBefore && event.getDml() == getTriggerEventDML()) {
                return event;
            }
        }
        throw new IllegalArgumentException();
    }

    /**
     * Get the nth trigger action sps UUID
     */
    public UUID getActionId(int index) {
        return actionSPSIdList.get(index);
    }

    /**
     * Get the trigger action sps UUID list
     */
    public List<UUID> getActionIdList() {
        return actionSPSIdList;
    }

    /**
     * Get the trigger action sps from SYSSTATEMENTS. If we find that
     * the sps is invalid and the trigger is defined at row level and it
     * has OLD/NEW transient variables through REFERENCES clause, then
     * the sps from SYSSTATEMENTS may not be valid anymore. In such a
     * case, we regenerate the trigger action sql and use that for the
     * sps and update SYSSTATEMENTS using this new sps. This update of
     * SYSSTATEMENTS was introduced with DERBY-4874
     *
     * @param lcc The LanguageConnectionContext to use.
     * @return the trigger action sps
     */
    public SPSDescriptor getActionSPS(LanguageConnectionContext lcc, int index) throws StandardException {

        return getSPS(lcc, index, null);

    }

    public SPSDescriptor getActionSPS(LanguageConnectionContext lcc, int index, ManagedCache<UUID, SPSDescriptor>localCache) throws StandardException {

        return getSPS(lcc, index, localCache);

    }

    /**
     * Get the SPS for the triggered SQL statement or the WHEN clause.
     *
     * @param lcc the LanguageConnectionContext to use
     * @param index {@code -1} if the SPS for the WHEN clause is
     *   requested, {@code >=0} if it is one of the triggered SQL statements
     * @return the requested SPS
     * @throws StandardException if an error occurs
     */
    private SPSDescriptor getSPS(LanguageConnectionContext lcc,
                                 int index, ManagedCache<UUID, SPSDescriptor>localCache)
            throws StandardException
    {
        boolean isWhenClause = index == -1;
        DataDictionary dd = getDataDictionary();
        UUID spsId = isWhenClause ? whenSPSId : actionSPSIdList.get(index);
        String originalSQL = isWhenClause ? whenClauseText : triggerDefinitionList.get(index);

        //bug 4821 - do the sysstatement look up in a nested readonly
        //transaction rather than in the user transaction. Because of
        //this, the nested compile transaction which is attempting to
        //compile the trigger will not run into any locking issues with
        //the user transaction for sysstatements.

        // KDW -- nested transaction for SPS retrieval not necessary for splice.  I hope.  This would
        // fail for row trigger that were executing on remote nodes with SpliceTransactionView with which we
        // cannot begin a new txn.
        //  lcc.beginNestedTransaction(true);
        SPSDescriptor sps = null;
        if (localCache != null)
            sps = localCache.getIfPresent(spsId);

        if (sps == null) {
            sps = dd.getSPSDescriptor(spsId);
            if (localCache != null && sps != null)
                localCache.put(spsId, sps);
        }

        assert sps != null : "sps should not be null";

        // lcc.commitNestedTransaction();

        //We need to regenerate the trigger action sql if
        //1)the trigger is found to be invalid,
        //2)the trigger is defined at row level (that is the only kind of
        //  trigger which allows reference to individual columns from
        //  old/new row)
        //3)the trigger action plan has columns that reference old/new row columns.
        //This code was added as part of DERBY-4874 where the Alter table
        //had changed the length of a varchar column from varchar(30) to
        //varchar(64) but the trigger action plan continued to use varchar(30).
        //To fix varchar(30) in trigger action sql to varchar(64), we need
        //to regenerate the trigger action sql. This new trigger action sql
        //will then get updated into SYSSTATEMENTS table.
        boolean usesReferencingClause = referencedColsInTriggerAction != null;

        if ((!sps.isValid() || (sps.getPreparedStatement() == null)) && isRow && usesReferencingClause) {
            SchemaDescriptor compSchema = dd.getSchemaDescriptor(triggerSchemaId, null);
            CompilerContext newCC = lcc.pushCompilerContext(compSchema);
            Parser pa = newCC.getParser();
            Visitable stmtnode =
                    isWhenClause ? pa.parseSearchCondition(originalSQL)
                                 : pa.parseStatement(originalSQL);
            lcc.popCompilerContext(newCC);
            int[] cols;
            cols = dd.examineTriggerNodeAndCols(stmtnode,
                    oldReferencingName,
                    newReferencingName,
                    referencedCols,
                    referencedColsInTriggerAction,
                    getTableDescriptor(),
                    null,
                    false);

            String newText = dd.getTriggerActionString(stmtnode,
                    oldReferencingName,
                    newReferencingName,
                    originalSQL,
                    referencedCols,
                    referencedColsInTriggerAction,
                    0,
                    getTableDescriptor(),
                    null,
                    false,
                    null,
                    cols);
            if (isWhenClause) {
                // The WHEN clause is not a full SQL statement, just a search
                // condition, so we need to turn it into a statement in order
                // to create an SPS.
                newText = "VALUES ( " + newText + " )";
            }
            sps.setText(newText);

            //By this point, we are finished transforming the trigger action if
            //it has any references to old/new transition variables.
        }

        return sps;
    }

    public int getNumBaseTableColumns() {
        if (numBaseTableColumns != 0)
            return numBaseTableColumns;
        else {
            try {
                return getTableDescriptor().getNumberOfColumns();
            }
            catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Get the trigger when clause sps UUID
     */
    public UUID getWhenClauseId() {
        return whenSPSId;
    }

    /**
     * Get the SQL text of the WHEN clause.
     * @return SQL text for the WHEN clause, or {@code null} if there is
     *   no WHEN clause
     */
    public String getWhenClauseText() {
        return whenClauseText;
    }

    /**
     * Get the trigger when clause sps
     */
    public SPSDescriptor getWhenClauseSPS(LanguageConnectionContext lcc, ManagedCache<UUID, SPSDescriptor> localCache) throws StandardException {
        if (whenSPSId == null) {
            // This trigger doesn't have a WHEN clause.
            return null;
        }
        return getSPS(lcc, -1, localCache);
    }

    /**
     * Get the trigger table descriptor
     *
     * @return the table descriptor upon which this trigger is declared
     */
    public TableDescriptor getTableDescriptor() throws StandardException {
        if (td == null) {
            td = getDataDictionary().getTableDescriptor(triggerTableId);
        }
        return td;
    }

    /**
     * Get the referenced column array for this trigger, used in "alter table drop column", we get the handle and change it
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9292")
    public int[] getReferencedCols() {
        return referencedCols;
    }

    /**
     * Get the referenced column array for the trigger action columns.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9292")
    public int[] getReferencedColsInTriggerAction() {
        return referencedColsInTriggerAction;
    }

    /**
     * Is this trigger enabled
     *
     * @return true if it is enabled
     */
    public boolean isEnabled() {
        return isEnabled;
    }

    /**
     * Mark this trigger as enabled
     */
    public void setEnabled() {
        isEnabled = true;
    }

    /**
     * Mark this trigger as disabled
     */
    public void setDisabled() {
        isEnabled = false;
    }

    /**
     * Does this trigger need to fire on this type of DML?
     *
     * @param stmtType     the type of DML (StatementType.INSERT|StatementType.UPDATE|StatementType.DELETE)
     * @param modifiedCols the columns modified, or null for all
     * @return true/false
     */
    public boolean needsToFire(int stmtType, int[] modifiedCols) throws StandardException {

        if (SanityManager.DEBUG) {
            if (!((stmtType == StatementType.INSERT) ||
                    (stmtType == StatementType.BULK_INSERT_REPLACE) ||
                    (stmtType == StatementType.UPDATE) ||
                    (stmtType == StatementType.DELETE))) {
                SanityManager.THROWASSERT("invalid statement type " + stmtType);
            }
        }

        /*
        ** If we are disabled, we never fire
        */
        if (!isEnabled) {
            return false;
        }

        if (stmtType == StatementType.INSERT) {
            return triggerDML == TriggerEventDML.INSERT;
        }
        if (stmtType == StatementType.DELETE) {
            return triggerDML == TriggerEventDML.DELETE;
        }

        // this is a temporary restriction, but it may not be lifted anytime soon.
        if (stmtType == StatementType.BULK_INSERT_REPLACE) {
            throw StandardException.newException(SQLState.LANG_NO_BULK_INSERT_REPLACE_WITH_TRIGGER,
                    getTableDescriptor().getQualifiedName(), name);
        }

        // if update, only relevant if columns intersect
        return triggerDML == TriggerEventDML.UPDATE && ConstraintDescriptor.doColumnsIntersect(modifiedCols, referencedCols);
    }

    /**
     * Get the original trigger definition.
     */
    public String getTriggerDefinition(int index) {
        return triggerDefinitionList.get(index);
    }

    public List<String> getTriggerDefinitionList() {
        return triggerDefinitionList;
    }

    public int getTriggerDefinitionSize() {
        return triggerDefinitionList.size();
    }

    /**
     * Get whether or not OLD was replaced in the REFERENCING clause.
     */
    public boolean getReferencingOld() {
        return referencingOld;
    }

    /**
     * Get whether or not NEW was replaced in the REFERENCING clause.
     */
    public boolean getReferencingNew() {
        return referencingNew;
    }

    /**
     * Get whether or not the trigger has a REFERENCING clause.
     */
    public boolean hasReferencingClause() {
        return getReferencingNew() || getReferencingOld();
    }

    /**
     * Get the old Referencing name, if any, from the REFERENCING clause.
     */
    public String getOldReferencingName() {
        return oldReferencingName;
    }

    /**
     * Get the new Referencing name, if any, from the REFERENCING clause.
     */
    public String getNewReferencingName() {
        return newReferencingName;
    }

    @Override
    public String toString() {
        return "name=" + name;
    }

    ////////////////////////////////////////////////////////////////////
    //
    // PROVIDER INTERFACE
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * @return the stored form of this provider
     * @see Dependable#getDependableFinder
     */
    @Override
    public DependableFinder getDependableFinder() {
        return getDependableFinder(StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID);
    }

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String    The name of this provider.
     */
    @Override
    public String getObjectName() {
        return name;
    }

    /**
     * Get the provider's UUID
     *
     * @return The provider's UUID
     */
    @Override
    public UUID getObjectID() {
        return id;
    }

    /**
     * Get the provider's type.
     *
     * @return char The provider's type.
     */
    @Override
    public String getClassType() {
        return Dependable.TRIGGER;
    }

    //////////////////////////////////////////////////////
    //
    // DEPENDENT INTERFACE
    //
    // Triggers are dependent on the underlying table,and their spses (for the trigger action and the WHEN clause).
    //
    //////////////////////////////////////////////////////

    /**
     * Check that all of the dependent's dependencies are valid.
     *
     * @return true if the dependent is currently valid
     */
    @Override
    public synchronized boolean isValid() {
        return true;
    }

    /**
     * Prepare to mark the dependent as invalid (due to at least one of its dependencies being invalid).
     *
     * @param action The action causing the invalidation
     * @param p      the provider
     * @param lcc    the language connection context
     * @throws StandardException thrown if unable to make it invalid
     */
    @Override
    public void prepareToInvalidate(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {
        switch (action) {
            /*
            ** We are only dependent on the underlying table, and our spses and
            ** privileges on various objects.  (we should be dropped before our 
            ** table is dropped. Also, we should be dropped before revoke 
            ** RESTRICT privilege is issued otherwise revoke RESTRICT will  
            ** throw an exception).
            ** Currently, in Derby, an execute routine privilege can be revoked
            ** only if there are no dependents on that privilege. When revoke 
            ** execute RESTRICT is exectued, all the dependents will receive
            ** REVOKE_PRIVILEGE_RESTRICT and they should throw exception. 
            ** We handle this for TriggerDescriptor by throwning an exception 
            ** below. For all the other types of revoke privileges, for 
            ** instance, SELECT, UPDATE, DELETE, INSERT, REFERENCES, 
            ** TRIGGER, we don't do anything here and later in makeInvalid, we 
            ** make the TriggerDescriptor drop itself. 
            */
            case DependencyManager.DROP_TABLE:
            case DependencyManager.DROP_SYNONYM:
            case DependencyManager.DROP_SPS:
            case DependencyManager.RENAME:
            case DependencyManager.REVOKE_PRIVILEGE_RESTRICT:
                DependencyManager dm = getDataDictionary().getDependencyManager();
                throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                        dm.getActionString(action), p.getObjectName(), "TRIGGER", name);
                        /*
            ** The trigger descriptor depends on the trigger table.
            ** This means that we get called whenever anything happens
            ** to the trigger table. There are so many cases where this
            ** can happen that it doesn't make sense to have an assertion
            ** here to check whether the action was expected (it makes
            ** the code hard to maintain, and creates a big switch statement).
            */
            default:
                break;
        }
    }

    /**
     * Mark the dependent as invalid (due to at least one of its dependencies being invalid).  Always an error
     * for a trigger -- should never have gotten here.
     *
     * @param lcc    the language connection context
     * @param action The action causing the invalidation
     * @throws StandardException thrown if called in sanity mode
     */
    @Override
    public void makeInvalid(int action, LanguageConnectionContext lcc) throws StandardException {
        // No sanity check for valid action. Trigger descriptors depend on
        // the trigger table, so there is a very large number of actions
        // that we would have to check against. This is hard to maintain,
        // so don't bother.

        switch (action) {
            // invalidate this trigger descriptor
            case DependencyManager.USER_RECOMPILE_REQUEST:
                DependencyManager dm = getDataDictionary().getDependencyManager();
                dm.invalidateFor(this, DependencyManager.PREPARED_STATEMENT_RELEASE, lcc);
                break;

            // When REVOKE_PRIVILEGE gets sent (this happens for privilege
            // types SELECT, UPDATE, DELETE, INSERT, REFERENCES, TRIGGER), we
            // make the TriggerDescriptor drop itself.
            // Ditto for revoking a role conferring a privilege.
            case DependencyManager.REVOKE_PRIVILEGE:
            case DependencyManager.REVOKE_ROLE:
                drop(lcc);
                lcc.getLastActivation().addWarning(StandardException.newWarning(SQLState.LANG_TRIGGER_DROPPED, this.getObjectName()));
                break;

            default:
                break;
        }

    }

    public void drop(LanguageConnectionContext lcc) throws StandardException {
        DataDictionary dd = getDataDictionary();
        DependencyManager dm = getDataDictionary().getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();
        dm.invalidateFor(this, DependencyManager.DROP_TRIGGER, lcc);

        // Drop the trigger
        dd.dropTriggerDescriptor(this, tc);

        // Clear the dependencies for the trigger 
        dm.clearDependencies(lcc, this);

        // Drop the spses
        for (UUID actionId: this.actionSPSIdList) {
            SPSDescriptor spsd = dd.getSPSDescriptor(actionId);

            // there shouldn't be any dependencies, but in case there are, lets clear them
            dm.invalidateFor(spsd, DependencyManager.DROP_TRIGGER, lcc);
            dm.clearDependencies(lcc, spsd);
            dd.dropSPSDescriptor(spsd, tc);
            // Remove all TECs from trigger stack. They will need to be rebuilt.
        }

        lcc.popAllTriggerExecutionContexts();

        if (getWhenClauseId() != null) {
            SPSDescriptor spsd = dd.getSPSDescriptor(getWhenClauseId());
            dm.invalidateFor(spsd, DependencyManager.DROP_TRIGGER, lcc);
            dm.clearDependencies(lcc, spsd);
            dd.dropSPSDescriptor(spsd, tc);
        }
    }


    //////////////////////////////////////////////////////////////
    //
    // FORMATABLE
    //
    //////////////////////////////////////////////////////////////

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     * @throws IOException            thrown on error
     * @throws ClassNotFoundException thrown on error
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Object obj;
        id = (UUID) in.readObject();
        name = (String) in.readObject();
        triggerSchemaId = (UUID) in.readObject();
        triggerTableId = (UUID) in.readObject();
        triggerDML = TriggerEventDML.fromId(in.readInt());
        isBefore = in.readBoolean();
        isRow = in.readBoolean();
        isEnabled = in.readBoolean();
        whenSPSId = (UUID) in.readObject();
        obj = in.readObject();
        if (obj instanceof UUID) {
            actionSPSIdList = new ArrayList<>();
            actionSPSIdList.add((UUID) obj);
        }
        int length = in.readInt();
        if (length != 0) {
            referencedCols = new int[length];
            for (int i = 0; i < length; i++) {
                referencedCols[i] = in.readInt();
            }
        }
        length = in.readInt();
        if (length != 0) {
            referencedColsInTriggerAction = new int[length];
            for (int i = 0; i < length; i++) {
                referencedColsInTriggerAction[i] = in.readInt();
            }
        }
        obj = in.readObject();
        if (obj instanceof String) {
            triggerDefinitionList = new ArrayList<>();
            triggerDefinitionList.add((String) obj);
        }
        referencingOld = in.readBoolean();
        referencingNew = in.readBoolean();
        oldReferencingName = (String) in.readObject();
        newReferencingName = (String) in.readObject();
        whenClauseText = (String) in.readObject();

    }

    @Override
    public DataDictionary getDataDictionary() {
        /*
           note: we need to do this since when this trigger is read back from
          disk (when it is associated with a sps), the dataDictionary has not 
           been initialized and therefore can give a NullPointerException
         */
        DataDictionary dd = super.getDataDictionary();
        if (dd == null) {
            LanguageConnectionContext lcc = (LanguageConnectionContext) ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
            dd = lcc.getDataDictionary();
            setDataDictionary(dd);
        }
        return dd;
    }

    /**
     * Write this object to a stream of stored objects.
     *
     * @param out write bytes here.
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(triggerSchemaId != null, "triggerSchemaId expected to be non-null");
            SanityManager.ASSERT(triggerTableId != null, "triggerTableId expected to be non-null");
        }
        out.writeObject(id);
        out.writeObject(name);
        out.writeObject(triggerSchemaId);
        out.writeObject(triggerTableId);
        out.writeInt(triggerDML.getId());
        out.writeBoolean(isBefore);
        out.writeBoolean(isRow);
        out.writeBoolean(isEnabled);
        out.writeObject(whenSPSId);
        out.writeObject(actionSPSIdList.size() == 1 ? actionSPSIdList.get(0) : null);
        if (referencedCols == null) {
            out.writeInt(0);
        } else {
            out.writeInt(referencedCols.length);
            for (int referencedCol : referencedCols) {
                out.writeInt(referencedCol);
            }
        }
        if (referencedColsInTriggerAction == null) {
            out.writeInt(0);
        } else {
            out.writeInt(referencedColsInTriggerAction.length);
            for (int aReferencedColsInTriggerAction : referencedColsInTriggerAction) {
                out.writeInt(aReferencedColsInTriggerAction);
            }
        }
        out.writeObject(triggerDefinitionList.size() == 1 ? triggerDefinitionList.get(0) : null);
        out.writeBoolean(referencingOld);
        out.writeBoolean(referencingNew);
        out.writeObject(oldReferencingName);
        out.writeObject(newReferencingName);
        out.writeObject(whenClauseText);
    }

    /**
     * Get the formatID which corresponds to this class.
     */
    @Override
    public int getTypeFormatId() {
        return StoredFormatIds.TRIGGER_DESCRIPTOR_V01_ID;
    }

    @Override
    public String getDescriptorType() {
        return "Trigger";
    }

    @Override
    public String getDescriptorName() {
        return name;
    }

}

