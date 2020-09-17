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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionStmtValidator;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import splice.com.google.common.cache.CacheBuilder;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * A trigger execution context (TEC) holds information that is available from the context of a trigger invocation.<br/>
 * A TEC is just a shell that gets pushed on a {@link TriggerExecutionStack} (initially empty). It is utilized by setting a
 * {@link TriggerDescriptor} on it and executing it's statement. After execution, the trigger descriptor is removed.<br/>
 * A TEC is removed from the stack when all of it's trigger executions, including any started by actions it may have
 * performed, have completed.
 * <p/>
 * A TEC will have to be serialized over synch boundaries so that trigger action information is available to
 * executions on other nodes.
 * <p/>
 * <h2>Possible Optimizations</h2>
 * <ul>
 * <li>We use a single row (for before/after row) implementation. This has changed from returning a heavy-weight
 * ResultSet to a lighter-weight ExecRow. We could support (for Statement Triggers) a batched up collection of
 * ExecRows.</li>
 * <li>A trigger rarely uses all columns in a row. We could use <code>changedColIds</code> to track only the required
 * new/old column values for the row.</li>
 * </ul>
 */
public class TriggerExecutionContext implements ExecutionStmtValidator, Externalizable {

    /* ========================
     * Serialized information
     * ========================
     */
    private int[] changedColIds;
    private String[] changedColNames;
    private String statementText;
    protected ConnectionContext cc;
    private UUID targetTableId;
    private String targetTableName;
    private ExecRow triggeringRow;
    private TriggerDescriptor triggerd;
    private ExecRow afterRow;   // used exclusively for InsertResultSets which have autoincrement columns.
    private TriggerEvent event;
    private FormatableBitSet heapList;
    private ExecRow execRowDefinition;
    private String tableVersion;
    private long conglomId;

    protected CursorResultSet  triggeringResultSet;
    private ManagedCache<UUID, SPSDescriptor> spsCache = null;
    private boolean fromSparkExecution;

    /*
    ** Used to track all the result sets we have given out to
    ** users.  When the trigger context is no longer valid,
    ** we close all the result sets that may be in the user
    ** space because they can no longer provide meaningful
    ** results.
    */
    @SuppressWarnings("UseOfObsoleteCollectionType")
    private Vector<ResultSet> resultSetVector;

    /**
     * aiCounters is a list of AutoincrementCounters used to keep state which might be used by the trigger. This is
     * only used by Insert triggers--Delete and Update triggers do not use this variable.
     */
    private List<AutoincrementCounter> aiCounters;

    /*
     * aiHT is a map of autoincrement <key, value> pairs. This is used for ai values generated by the trigger.
     */
    private Map<String, Long> aiHT;

    public TriggerExecutionContext() {}

    /**
     * Build me a big old nasty trigger execution context. Damnit.
     *
     * @param statementText   the text of the statement that caused the trigger to fire.  may be null if we are replicating
     * @param changedColIds   the list of columns that changed.  Null for all columns or INSERT/DELETE.
     * @param changedColNames the names that correspond to changedColIds
     * @param targetTableId   the UUID of the table upon which the trigger fired
     * @param targetTableName the name of the table upon which the trigger fired
     * @param aiCounters      A list of AutoincrementCounters to keep state of the ai columns in this insert trigger.
     */
    public TriggerExecutionContext(ConnectionContext cc,
                                   String statementText,
                                   int[] changedColIds,
                                   String[] changedColNames,
                                   UUID targetTableId,
                                   String targetTableName,
                                   List<AutoincrementCounter> aiCounters,
                                   FormatableBitSet heapList,
                                   boolean fromSparkExecution) throws StandardException {

        this.changedColIds = changedColIds;
        this.changedColNames = changedColNames;
        this.statementText = statementText;
        this.targetTableId = targetTableId;
        this.targetTableName = targetTableName;
        this.aiCounters = aiCounters;
        this.heapList = heapList;
        this.resultSetVector = new Vector<>();
        this.cc = cc;

        this.fromSparkExecution = fromSparkExecution;
        // only use the local cache for spark execution
        if (fromSparkExecution)
            this.spsCache = new ManagedCache<>(CacheBuilder.newBuilder().recordStats().maximumSize(100).build(), 100);

        if (SanityManager.DEBUG) {
            if ((changedColIds == null) != (changedColNames == null)) {
                SanityManager.THROWASSERT("bad changed cols, " +
                        "(changedColsIds == null) = " + (changedColIds == null) +
                        "  (changedColsNames == null) = " + (changedColNames == null));
            }
            if (changedColIds != null) {
                SanityManager.ASSERT(changedColIds.length == (changedColNames != null ? changedColNames.length : 0),
                        "different number of changed col ids vs names");
            }
        }
    }
    // Push a LanguageConnectionContext into
    // the task's ContextManager, if needed.  Return true if the push was done.
    public static boolean
    pushLanguageConnectionContextToCM(LanguageConnectionContext newLCC, ContextManager cm)
                                                               throws StandardException  {
        boolean lccPushed = false;
        try {
            LanguageConnectionContext currentLCC = ConnectionUtil.getCurrentLCC();
            if (newLCC != null) {
                if (newLCC != currentLCC) {
                    cm.pushContext(newLCC);
                    lccPushed = true;
                }
            }
        } catch (SQLException e) {
            // If the current LCC is not available in the context,
            // push it now.
            if (newLCC != null) {
                lccPushed = true;
                cm.pushContext(newLCC);
            }
        }
        return lccPushed;
    }

    // Push the LanguageConnectionContext from the Activation into
    // the task's ContextManager, if needed.  Return true if the push was done.
    public static boolean
    pushLanguageConnectionContextFromActivation(Activation activation, ContextManager cm)
                                                               throws StandardException  {
        boolean lccPushed = false;
        try {
            LanguageConnectionContext currentLCC = ConnectionUtil.getCurrentLCC();
            if (activation != null && activation.getLanguageConnectionContext() != null) {
                LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
                if (lcc != currentLCC) {
                    cm.pushContext(lcc);
                    lccPushed = true;
                }
            }
        } catch (SQLException e) {
            // If the current LCC is not available in the context,
            // push it now.
            if (activation != null && activation.getLanguageConnectionContext() != null) {
                lccPushed = true;
                cm.pushContext(activation.getLanguageConnectionContext());
            }
        }
        return lccPushed;
    }

    // Push the TriggerExecutionContext from the Activation into
    // the task's current LCC.  Return true if a new LCC was
    // was pushed to the task's ContextManager.
    public static boolean
    pushTriggerExecutionContextFromActivation(Activation activation, ContextManager cm)
                                                               throws StandardException  {
        boolean lccPushed = false;
        try {
            LanguageConnectionContext currentLCC = ConnectionUtil.getCurrentLCC();
            if (activation != null && activation.getLanguageConnectionContext() != null) {
                LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
                if (lcc != currentLCC &&
                    lcc.getTriggerExecutionContext() != null) {
                    if (currentLCC.getTriggerExecutionContext() != null)
                        currentLCC.popTriggerExecutionContext(currentLCC.getTriggerExecutionContext());
                    currentLCC.pushTriggerExecutionContext(lcc.getTriggerExecutionContext());
                }
            }
        } catch (SQLException e) {
            // If the current LCC is not available in the context,
            // push it now.
            if (activation != null && activation.getLanguageConnectionContext() != null) {
                lccPushed = true;
                cm.pushContext(activation.getLanguageConnectionContext());
            }
        }
        return lccPushed;
    }

    public boolean currentTriggerHasReferencingClause() {
        if (triggerd == null)
            return false;
        return triggerd.hasReferencingClause();
    }

    public boolean statementTriggerWithReferencingClause() {
        if (triggerd == null)
            return false;
        return triggerd.hasReferencingClause() && !triggerd.isRowTrigger();
    }

    public long getConglomId() {
        return conglomId;
    }

    public ExecRow getExecRowDefinition() {
        return execRowDefinition;
    }

    public String getTableVersion() {
        return tableVersion;
    }

    public void setExecRowDefinition(ExecRow execRowDefinition) {
        this.execRowDefinition = execRowDefinition;
    }

    public void setTableVersion(String tableVersion) {
        this.tableVersion = tableVersion;
    }

    public void setConglomId(long conglomId) {
        this.conglomId = conglomId;
    }

    public boolean hasTriggeringResultSet() { return triggeringResultSet != null; }

    public void setTriggeringResultSet(CursorResultSet rs) throws StandardException {
        triggeringResultSet = rs;
        if (rs == null) {
            return;
        }

        if (aiCounters != null) {
            if (triggerd.isRowTrigger()) {
                // An after row trigger needs to see the "first" row inserted
                rs.open();
                afterRow = rs.getNextRow();
                rs.close();
            } else {
                // after statement trigger needs to look at the last value.
                if (!triggerd.isBeforeTrigger()) {
                    resetAICounters(false);
                }
            }
        }
        if (rs.isClosed())
            rs.open();
        triggeringRow = rs.getCurrentRow();
        try {
            if (triggerd.isRowTrigger())
                rs.close();
        } catch (StandardException e) {
            // ignore - close quietly.
        }
    }

    public void setCurrentTriggerEvent(TriggerEvent event) {
        this.event = event;
    }

    public void setTrigger(TriggerDescriptor triggerd) {
        this.triggerd = triggerd;
    }

    public void clearTrigger(boolean deferCleanup) throws StandardException {
        event = null;
        triggerd = null;
        triggeringRow = null;
        if (!deferCleanup)
            cleanup();
    }

    /////////////////////////////////////////////////////////
    //
    // ExecutionStmtValidator
    //
    /////////////////////////////////////////////////////////

    /**
     * Make sure that whatever statement is about to be executed is ok from the context of this trigger.
     * <p/>
     * Note that we are sub classed in replication for checks for replication specific language.
     *
     * @param constantAction the constant action of the action that we are to validate
     */
    @Override
    public void validateStatement(ConstantAction constantAction) throws StandardException {

        // DDL statements are not allowed in triggers. Direct use of DDL
        // statements in a trigger's action statement is disallowed by the
        // parser. However, this runtime check is needed to prevent execution
        // of DDL statements by procedures within a trigger context.
        if (constantAction instanceof DDLConstantAction) {
            throw StandardException.newException(SQLState.LANG_NO_DDL_IN_TRIGGER, triggerd.getName(), constantAction.toString());
        }

        // No INSERT/UPDATE/DELETE for a before trigger. There is no need to
        // check this here because parser does not allow these DML statements
        // in a trigger's action statement in a before trigger. Parser also
        // disallows creation of before triggers calling procedures that modify
        // SQL data.

    }

    /////////////////////////////////////////////////////////
    //
    // TriggerExectionContext
    //
    /////////////////////////////////////////////////////////

    /**
     * Get the target table name upon which the trigger event is declared.
     *
     * @return the target table
     */
    public String getTargetTableName() {
        return targetTableName;
    }

    /**
     * Get the target table UUID upon which the trigger event is declared.
     *
     * @return the uuid of the target table
     */
    public UUID getTargetTableId() {
        return targetTableId;
    }


    /**
     * Get the text of the statement that caused the trigger to fire.
     *
     * @return the statement text
     */
    public String getEventStatementText() {
        return statementText;
    }

    /**
     * Get the columns that have been modified by the statement that caused this trigger to fire.  If all columns are
     * modified, will return null (e.g. for INSERT or DELETE will return null).
     *
     * @return an array of Strings
     */
    public String[] getModifiedColumns() {
        return changedColNames;
    }

    /**
     * Find out of a column was changed, by column name
     *
     * @param columnName the column to check
     * @return true if the column was modified by this statement.
     * Note that this will always return true for INSERT and DELETE regardless of the column name passed in.
     */
    public boolean wasColumnModified(String columnName) {
        if (changedColNames == null) {
            return true;
        }
        for (String changedColName : changedColNames) {
            if (changedColName.equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Find out of a column was changed, by column number
     *
     * @param columnNumber the column to check
     * @return true if the column was modified by this statement.
     * Note that this will always return true for INSERT and DELETE regardless of the column name passed in.
     */
    public boolean wasColumnModified(int columnNumber) {
        if (changedColIds == null) {
            return true;
        }
        for (int i = 0; i < changedColNames.length; i++) {
            if (changedColIds[i] == columnNumber) {
                return true;
            }
        }
        return false;
    }

    public void setConnectionContext(ConnectionContext cc) {
        this.cc = cc;
    }

    /**
     * Returns a result set row the new images of the changed rows. For a row trigger, the result set will have a
     * single row.  For a statement trigger, this result set has every row that has changed or will change.  If a
     * statement trigger does not affect a row, then the result set will be empty (i.e. ResultSet.next()
     * will return false).
     *
     * @return the ResultSet containing after images of the rows changed by the triggering event.
     * @throws SQLException if called after the triggering event has completed
     */
    public ResultSet getNewRowSet() throws SQLException
	{
		if (triggeringResultSet == null)
		{
                    return null;
		}
		try
		{
			/* We should really shallow clone the result set, because it could be used
			 * at multiple places independently in trigger action.  This is a bug found
			 * during the fix of beetle 4373.
			 */
			CursorResultSet ars = triggeringResultSet;
			if (ars instanceof TemporaryRowHolderResultSet)
				ars = (CursorResultSet) ((TemporaryRowHolderResultSet) ars).clone();
			ars.open();
			java.sql.ResultSet rs = cc.getResultSet(ars);
			resultSetVector.addElement(rs);
			return rs;
		} catch (StandardException se)
		{
			throw PublicAPI.wrapStandardException(se);
		}
	}

    /**
     * Like getBeforeResultSet(), but returns a result set positioned on the first row of the before result set.
     * Used as a convenience to get a column for a row trigger.  Equivalent to getBeforeResultSet() followed by next().
     *
     * @return the ResultSet positioned on the old row image.
     * @throws SQLException if called after the triggering event has completed
     */
    public ExecRow getOldRow() throws SQLException {
        return buildOldRow(triggeringRow, false);
    }

    public ExecRow buildOldRow(ExecRow sourceRow, boolean forStatementTrigger) throws SQLException {
        if (sourceRow == null) {
            return null;
        }
        if (this.event != null && this.event.isUpdate()) {
            return extractColumns(sourceRow, true, forStatementTrigger);
        }
        return sourceRow;
    }

    public ExecRow getNewRow() throws SQLException {
        return buildNewRow(triggeringRow, false);
    }

    public ExecRow buildNewRow(ExecRow sourceRow, boolean forStatementTrigger) throws SQLException {
        if (sourceRow == null) {
            return null;
        }
        if (this.event != null && this.event.isUpdate()) {
            return extractColumns(sourceRow, false, forStatementTrigger);
        }
        return sourceRow;
    }


    public Long getAutoincrementValue(String identity) {
        // first search the map-- this represents the ai values generated by this trigger.
        if (aiHT != null) {
            Long value = aiHT.get(identity);
            if (value != null)
                return value;
        }

        // If we didn't find it in the map search in the counters which
        // represent values inherited by trigger from insert statements.
        if (aiCounters != null) {
            for (AutoincrementCounter aic : aiCounters) {
                if (identity.equals(aic.getIdentity())) {
                    return aic.getCurrentValue();
                }
            }
        }

        // didn't find it-- return NULL.
        return null;
    }

    /**
     * Copy a map of autoincrement values into the trigger execution context map of autoincrement values.
     */
    public void copyMapToAIHT(Map<String, Long> from) {
        if (from == null) {
            return;
        }
        if (aiHT == null) {
            aiHT = new HashMap<>();
        }
        aiHT.putAll(from);
    }

    /**
     * Reset Autoincrement counters to the beginning or the end.
     *
     * @param begin if True, reset the AutoincremnetCounter to the
     *              beginning-- used to reset the counters for the
     *              next trigger. If false, reset it to the end--
     *              this sets up the counter appropriately for a
     *              AFTER STATEMENT trigger.
     */
    public void resetAICounters(boolean begin) {
        if (aiCounters == null) {
            return;
        }

        afterRow = null;
        for (AutoincrementCounter aic : aiCounters) {
            aic.reset(begin);
        }
    }

    /**
     * Update Autoincrement Counters from the last row inserted.
     */
    public void updateAICounters() throws StandardException {
        if (aiCounters == null) {
            return;
        }
        for (AutoincrementCounter aic : aiCounters) {
            DataValueDescriptor dvd = afterRow.getColumn(aic.getColumnPosition());
            aic.update(dvd.getLong());
        }
    }

    @Override
    public String toString() {
        return "Name="+targetTableName+" triggerd=" + Objects.toString(triggerd);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ArrayUtil.writeIntArray(out, changedColIds);
        ArrayUtil.writeArray(out, changedColNames);
        out.writeObject(statementText);
        out.writeObject(targetTableId);
        out.writeObject(targetTableName);
        out.writeObject(triggerd);
        out.writeObject(triggeringRow);
        out.writeObject(afterRow);
        if (event != null) {
            out.writeBoolean(true);
            out.writeInt(event.ordinal());
        } else {
            out.writeBoolean(false);
        }
        out.writeObject(heapList);
        boolean hasExecRowDefinition = execRowDefinition != null;
        out.writeBoolean(hasExecRowDefinition);
        if (hasExecRowDefinition)
            out.writeObject(execRowDefinition);
        boolean hasTableVersion = tableVersion != null;
        out.writeBoolean(hasTableVersion);
        if (hasTableVersion)
            out.writeUTF(tableVersion);
        out.writeLong(conglomId);
        out.writeBoolean(fromSparkExecution);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        changedColIds = ArrayUtil.readIntArray(in);
        int len = ArrayUtil.readArrayLength(in);
        if (len != 0) {
            changedColNames = new String[len];
            ArrayUtil.readArrayItems(in, changedColNames);
        }
        statementText = (String) in.readObject();
        targetTableId = (UUID) in.readObject();
        targetTableName = (String) in.readObject();
        triggerd = (TriggerDescriptor) in.readObject();
        triggeringRow = (ExecRow) in.readObject();
        afterRow = (ExecRow) in.readObject();
        if (in.readBoolean()) {
            event = TriggerEvent.values()[in.readInt()];
        }
        heapList = (FormatableBitSet)in.readObject();
        this.resultSetVector = new Vector<>();
        if (in.readBoolean())
            execRowDefinition = (ExecRow) in.readObject();
        if (in.readBoolean())
            tableVersion = in.readUTF();
        conglomId = in.readLong();
        fromSparkExecution = in.readBoolean();
        if (fromSparkExecution)
            spsCache = new ManagedCache<>(CacheBuilder.newBuilder().recordStats().maximumSize(100).build(), 100);
    }

    /**
     * Extract the appropriate columns from the given row.
     * <p/>
     * This method is used for update row triggers to get the appropriate transition values.<br/>
     * When an update row trigger fires, we get a triggering result row for each row modified. This
     * row (resultSet) will have both the old and new transition values in a form such as,
     * <code>[old1, old2, ..., new1, new2, ..., {rowKey}]</code> (the rowKey) is appended.<br/>
     * The transition row is always of size (number of columns in table * 2 + 1).
     *
     * @param resultSet the current triggering row
     * @param firstHalf if true, return an ExecRow with the first (front) half of the columns in
     *                  the given resultSet. If false, return the back half.
     * @return an update exec row with either old or new transition values.
     * @throws SQLException
     */
    private ExecRow extractColumns(ExecRow resultSet, boolean firstHalf, boolean forStatementTrigger) throws SQLException {
        if (!forStatementTrigger && heapList != null && triggerd.getReferencedCols() != null)
            return extractTriggerColumns(resultSet, firstHalf);
        else {
            return extractAllColumns(resultSet, firstHalf);
        }

    }

    /**
     * If the update trigger does not refer any particular column, the old and new full row is available. Simply extract
     * first half or second half according to whether NEW or OLD row is referenced in trigger action
     * @param resultSet
     * @param firstHalf
     * @return
     * @throws SQLException
     */
    private ExecRow extractAllColumns(ExecRow resultSet, boolean firstHalf) throws SQLException {
        int nCols = (resultSet.nColumns() - 1) / 2;
        ExecRow result = new ValueRow(nCols);
        int sourceIndex = (firstHalf ? 1 : nCols + 1);
        int stopIndex = (firstHalf ? nCols : (resultSet.nColumns()-1));
        int targetIndex = 1;
        for (; sourceIndex<=stopIndex; sourceIndex++) {
            try {
                result.setColumn(targetIndex++, resultSet.getColumn(sourceIndex));
            } catch (StandardException e) {
                throw Util.generateCsSQLException(e);
            }
        }
        return result;
    }

    /**
     * If the update trigger references a column, execRow only contains columns that are changed and referenced by
     * trigger action. Extract columns according to heapList and triggerDescriptor
     * @param resultSet
     * @param firstHalf
     * @return
     * @throws SQLException
     */
    private ExecRow extractTriggerColumns(ExecRow resultSet, boolean firstHalf) throws SQLException {
        int[] referencedCols = triggerd.getReferencedCols();
        int[] referencedColsInTriggerAction = triggerd.getReferencedColsInTriggerAction();
        int nResultCols = (referencedCols!=null?referencedCols.length:0) +
                (referencedColsInTriggerAction!=null?referencedColsInTriggerAction.length:0);
        ExecRow result = new ValueRow(nResultCols);
        try {
            FormatableBitSet allTriggerColumns = new FormatableBitSet(triggerd.getNumBaseTableColumns()+1);
            if (referencedCols != null) {
                for (int col : referencedCols) {
                    allTriggerColumns.set(col);
                }
            }
            if (referencedColsInTriggerAction != null) {
                for (int col : referencedColsInTriggerAction) {
                    allTriggerColumns.set(col);
                }
            }
            int nCols = resultSet.nColumns()/2;
            int sourceIndex = (firstHalf ? 1 : nCols + 1);
            int pos = 1;

            for (int i = allTriggerColumns.anySetBit(); i >= 0; i = allTriggerColumns.anySetBit(i)) {
                int index = 0;
                for (int j = heapList.anySetBit(); j >= 0; j = heapList.anySetBit(j)) {
                    if (j == i) {
                        result.setColumn(pos++, resultSet.getColumn(sourceIndex + index));
                    }
                    index++;
                }
            }
        }catch (StandardException e) {
            throw Util.generateCsSQLException(e);
        }
        return result;
    }

    /**
     * Cleanup the trigger execution context.  <B>MUST</B>
     * be called when the caller is done with the trigger
     * execution context.
     * <p>
     * We go to somewhat exaggerated lengths to free up
     * all our resources here because a user may hold on
     * to a TEC after it is valid, so we clean everything
     * up to be on the safe side.
     *
     * @exception StandardException on unexpected error
     */
    protected void cleanup()
            throws StandardException
    {
        /*
        ** Explicitly close all result sets that we have
        ** given out to the user.
        */
        if (resultSetVector != null) {
            for (ResultSet rs : resultSetVector) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }
            
            resultSetVector.clear();
        }


        /*
        ** We should have already closed our underlying
        ** ExecResultSets by closing the jdbc result sets,
        ** but in case we got an error that we caught and
        ** ignored, explicitly close them.
        */
        if (triggeringResultSet != null)
        {
            triggeringResultSet.close();
        }
    }

    public SPSDescriptor getSPSDescriptor(UUID uuid) {
        return spsCache.getIfPresent(uuid);
    }

    public void setSPSDescriptor(SPSDescriptor desc) {
        spsCache.put(desc.getUUID(), desc);
    }

    public ManagedCache<UUID, SPSDescriptor> getSpsCache() {
        return spsCache;
    }
}
