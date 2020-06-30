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
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.Statement;
import com.splicemachine.db.iapi.sql.StorablePreparedStatement;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionFactory;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * A SPSDescriptor describes a Stored Prepared Statement. It correlates to a row in SYS.SYSSTATEMENTS.
 * <p/>
 * <B>SYNCHRONIZATION</B>: Stored prepared statements may be cached.  Thus they may be shared by multiple threads.  It
 * is very hard for two threads to try to muck with an sps simultaneously because all ddl (including sps recompilation)
 * clears out the sps cache and invalidates whatever statement held a cached sps. But it is possible for two statements
 * to do a prepare execute statement <x> at the exact same time, so both try to do an sps.prepare() at the same time
 * during code generation, so we synchronize most everything except getters on immutable objects just to be on the safe
 * side.
 */
public class SPSDescriptor extends TupleDescriptor implements UniqueSQLObjectDescriptor, Dependent, Provider {
    /**
     * Statement types.
     * <UL>
     * <LI> SPS_TYPE_TRIGGER    - trigger</LI>
     * <LI> SPS_TYPE_EXPLAIN    - explain (<B>NOT IMPLEMENTED</B>) </LI>
     * <LI> SPS_TYPE_REGULAR    - catchall</LI>
     * </UL>
     */
    public static final char SPS_TYPE_TRIGGER = 'T';
    public static final char SPS_TYPE_REGULAR = 'S';
    public static final char SPS_TYPE_EXPLAIN = 'X';

    private static final int RECOMPILE = 1;
    private static final int INVALIDATE = 0;

    // Class contents
    private final SchemaDescriptor sd;
    private final String name;
    private final UUID compSchemaId;
    private final char type;
    private String text;
    private final String usingText;
    private final UUID uuid;

    private boolean valid;
    private ExecPreparedStatement preparedStatement;
    private DataTypeDescriptor params[];
    private Timestamp compileTime;
    /**
     * Old code - never used.
     */
    private Object paramDefaults[];
    private boolean initiallyCompilable;
    private boolean lookedUpParams;

    private UUIDFactory uuidFactory;


    // constructors

    /**
     * Constructor for a SPS Descriptor
     *
     * @param dataDictionary      The data dictionary that this descriptor lives in
     * @param name                the SPS name
     * @param uuid                the UUID
     * @param suuid               the schema UUID
     * @param compSchemaUUID      the schema UUID at compilation time
     * @param valid               is the sps valid
     * @param text                the text for this statement
     * @param type                type
     * @param initiallyCompilable is the statement initially compilable?
     */
    public SPSDescriptor(DataDictionary dataDictionary,
                         String name,
                         UUID uuid,
                         UUID suuid,
                         UUID compSchemaUUID,
                         char type,
                         boolean valid,
                         String text,
                         boolean initiallyCompilable) throws StandardException {
        this(dataDictionary, name, uuid, suuid, compSchemaUUID, type, valid, text, null, null, null, initiallyCompilable);
    }

    /**
     * Constructor for a SPS Descriptor.  Used when constructing an SPS descriptor from a row in SYSSTATEMENTS.
     *
     * @param name                the SPS name
     * @param uuid                the UUID
     * @param suuid               the schema UUID
     * @param compSchemaUUID      the schema UUID at compilation time
     * @param valid               is the sps valid
     * @param text                the text for this statement
     * @param usingText           the text for the USING clause supplied to
     *                            CREATE or ALTER STATEMENT
     * @param compileTime         the time this was compiled
     * @param preparedStatement   the PreparedStatement
     * @param dataDictionary      The data dictionary that this descriptor lives in
     * @param type                type
     * @param initiallyCompilable is the statement initially compilable?
     */
    @SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "IS2_INCONSISTENT_SYNC"}, justification = "DB-9289 - DB-9290")
    public SPSDescriptor(DataDictionary dataDictionary,
                         String name,
                         UUID uuid,
                         UUID suuid,
                         UUID compSchemaUUID,
                         char type,
                         boolean valid,
                         String text,
                         String usingText,
                         Timestamp compileTime,
                         ExecPreparedStatement preparedStatement,
                         boolean initiallyCompilable) throws StandardException {
        super(dataDictionary);

        // Added this check when setUUID was removed, see DERBY-4918.
        if (uuid == null) {
            throw new IllegalArgumentException("UUID is null");
        }
        this.name = name;
        this.uuid = uuid;
        this.type = type;
        this.text = text;
        this.usingText = usingText;
        this.valid = valid;
        this.compileTime = compileTime;
        this.sd = dataDictionary.getSchemaDescriptor(suuid, null);
        this.preparedStatement = preparedStatement;
        this.compSchemaId = compSchemaUUID;
        this.initiallyCompilable = initiallyCompilable;
    }

    /**
     * FOR TRIGGERS ONLY
     * <p/>
     * Generate the class for this SPS and immediately release it.  This is useful for cases where we don't want to
     * immediately execute the statement corresponding to this sps (e.g. CREATE STATEMENT).
     * <p/>
     * <I>SIDE EFFECTS</I>: will update and SYSDEPENDS with the prepared statement dependency info.
     *
     * @param lcc          the language connection context
     * @param triggerTable the table descriptor to bind against.  Had
     *                     better be null if this isn't a trigger sps.
     * @param tc           the transaction controller
     */
    public final synchronized void prepareAndRelease(LanguageConnectionContext lcc,
                                                     TableDescriptor triggerTable,
                                                     TransactionController tc) throws StandardException {
        if (SanityManager.DEBUG) {
            if (triggerTable != null) {
                SanityManager.ASSERT(type == SPS_TYPE_TRIGGER, "only expect a table descriptor when we have a trigger");
            }
        }
        compileStatement(lcc, triggerTable, tc);
        preparedStatement.makeInvalid(DependencyManager.PREPARED_STATEMENT_RELEASE, lcc);
    }

    /**
     * FOR TRIGGERS ONLY
     * <p/>
     * Generate the class for this SPS and immediately release it.  This is useful for cases where we don't want to
     * immediately execute the statement corresponding to this sps (e.g. CREATE STATEMENT).
     * <p/>
     * <I>SIDE EFFECTS</I>: will update and SYSDEPENDS with the prepared statement dependency info.
     *
     * @param lcc          the language connection context
     * @param triggerTable the table descriptor to bind against.  Had
     *                     better be null if this isn't a trigger sps.
     */
    public final synchronized void prepareAndRelease(LanguageConnectionContext lcc,
                                                     TableDescriptor triggerTable) throws StandardException {
        prepareAndRelease(lcc, triggerTable, null);
    }

    /**
     * Generate the class for this SPS and immediately release it.  This is useful for cases where we don't want to
     * immediately execute the statement corresponding to this sps (e.g. CREATE STATEMENT).
     * <p/>
     * <I>SIDE EFFECTS</I>: will update and SYSDEPENDS with the prepared statement dependency info.
     *
     * @param lcc the language connection context
     */
    public final synchronized void prepareAndRelease(LanguageConnectionContext lcc) throws StandardException {
        prepareAndRelease(lcc, (TableDescriptor) null, (TransactionController) null);
    }

    /**
     * Compiles this SPS.
     * <p/>
     * <em>Note:</em> This SPS may still be marked as invalid after this method has completed, because an invalidation
     * request may have been received while compiling.
     *
     * @param lcc          connection
     * @param triggerTable subject table (may be {@code null})
     * @param tc           transaction controller to use (may be {@code null})
     */
    private void compileStatement(LanguageConnectionContext lcc,
                                  TableDescriptor triggerTable,
                                  TransactionController tc) throws StandardException {
        ContextManager cm = lcc.getContextManager();
        LanguageConnectionFactory lcf = lcc.getLanguageConnectionFactory();
        DataDictionary dd = getDataDictionary();

        /* If we are a trigger, then we have to go ahead and locate the trigger's table descriptor and push it on the
         * lcc.  This is expensive, but pretty atypical since trigger actions aren't likely to be invalidated too often.
         * Also, when possible, we already have the triggerTable. */
        if (type == SPS_TYPE_TRIGGER && triggerTable == null) {
            // name consists of (see CreateTriggerConstantAction): TRIGGER<ACTN|WHEN>_<UUID:36>_<UUID:36>
            String uuidStr = name.split("_")[2];
            triggerTable = dd.getTableDescriptor(recreateUUID(uuidStr));
            if (SanityManager.DEBUG) {
                if (triggerTable == null) {
                    SanityManager.THROWASSERT("couldn't find trigger table for trigger sps " + name);
                }
            }
        }

        if (triggerTable != null) {
            lcc.pushTriggerTable(triggerTable);
        }

        // stored statements always stored as unicode.
        Statement stmt = lcf.getStatement(dd.getSchemaDescriptor(compSchemaId, null), text, true, lcc);

        try {
            preparedStatement = (ExecPreparedStatement) stmt.prepareStorable(
                    lcc,
                    preparedStatement,
                    getParameterDefaults(),
                    getSchemaDescriptor(),
                    type == SPS_TYPE_TRIGGER);
        } finally {
            if (triggerTable != null) {
                lcc.popTriggerTable(triggerTable);
            }
        }

        // If this references a SESSION schema table (temporary or permanent), then throw an exception
        // This is if EXECUTE STATEMENT executing a statement that was created with NOCOMPILE. Because
        // of NOCOMPILE, we could not catch SESSION schema table reference by the statement at
        // CREATE STATEMENT time. And hence need to catch such statements at EXECUTE STATEMENT time
        // when the query is getting compiled.
        if (preparedStatement.referencesSessionSchema()) {
            throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
        }

        setCompileTime();
        setParams(preparedStatement.getParameterTypes());

        String role = lcc.getReplicationRole();
        if (!dd.isReadOnlyUpgrade() && role.compareToIgnoreCase("REPLICA") != 0 &&
                lcc.getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.SNAPSHOT_TIMESTAMP) == null) {

            dd.startWriting(lcc);

            DependencyManager dm = dd.getDependencyManager();

            /* Clear out all the dependencies that exist before we recreate them so we don't grow SYS.SYSDEPENDS forever.*/
            dm.clearDependencies(lcc, this, tc);

            /* Copy over all the dependencies to me */
            dm.copyDependencies(preparedStatement,    // from
                    this,    // to
                    false,    // persistent only
                    cm,
                    tc);

            //If this sps is for a trigger action, then add the dependency between this sps and the trigger table DERBY-5120
            if (triggerTable != null) {
                dm.addDependency(this, triggerTable, lcc.getContextManager());
            }
        }

        // mark it as valid
        valid = true;
    }

    /**
     * Gets the name of the sps.
     */
    @Override
    public final String getName() {
        return name;
    }

    /**
     * Gets the full, qualified name of the statement.
     */
    public final String getQualifiedName() {
        return sd.getSchemaName() + "." + name;
    }

    /**
     * Gets the SchemaDescriptor for this SPS Descriptor.
     */
    @Override
    public final SchemaDescriptor getSchemaDescriptor() {
        return sd;
    }

    /**
     * Gets an identifier telling what type of table this is. Types match final ints in this interface.  Currently
     * returns SPS_TYPE_REGULAR or SPS_TYPE_TRIGGER.
     */
    public final char getType() {
        return type;
    }

    /**
     * Simple little helper function to convert your type to a string, which is easier to use.
     */
    public final String getTypeAsString() {
        return String.valueOf(type);
    }

    /**
     * Is the statement initially compilable?
     *
     * @return false if statement was created with the NOCOMPILE flag true otherwise
     */
    public boolean initiallyCompilable() {
        return initiallyCompilable;
    }

    /**
     * Validate the type. <B>NOTE</B>: Only SPS_TYPE_REGULAR and SPS_TYPE_TRIGGER are currently valid.
     *
     * @param type the type
     */
    public static boolean validType(char type) {
        return (type == SPSDescriptor.SPS_TYPE_REGULAR) || (type == SPSDescriptor.SPS_TYPE_TRIGGER);
    }

    /**
     * The time this prepared statement was compiled
     *
     * @return the time this class was last compiled
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9289")
    public final synchronized Timestamp getCompileTime() {
        return compileTime;
    }

    /**
     * Set the compile time to now
     */
    public final synchronized void setCompileTime() {
        compileTime = new Timestamp(System.currentTimeMillis());
    }

    /**
     * Get the text used to create this statement. Returns original text in a cleartext string.
     *
     * @return The text
     */
    public final synchronized String getText() {
        return text;
    }

    /**
     * It is possible that when a trigger is invalidated, the generated trigger
     * action sql associated with it needs to be regenerated. One example
     * of such a case would be when ALTER TABLE on the trigger table
     * changes the length of a column. The need for this code was found
     * as part of DERBY-4874 where the Alter table had changed the length
     * of a varchar column from varchar(30) to varchar(64) but the generated
     * trigger action plan continued to use varchar(30). To fix varchar(30) in
     * in trigger action sql to varchar(64), we need to regenerate the
     * trigger action sql which is saved as stored prepared statement. This
     * new trigger action sql will then get updated into SYSSTATEMENTS table.
     * DERBY-4874
     */
    public final synchronized void setText(String newText) {
        text = newText;
    }

    /**
     * Get the text of the USING clause used on CREATE or ALTER statement.
     */
    public final String getUsingText() {
        return usingText;
    }

    /**
     * Gets the UUID of the SPS.
     */
    @Override
    public final UUID getUUID() {
        return uuid;
    }

    /**
     * Get the array of date type descriptors for
     * this statement.  Currently, we do a lookup
     * if we don't already have the parameters saved.
     * When SPSes are cached, the parameters should
     * be set up when the sps is constructed.
     *
     * @return the array of data type descriptors
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9289")
    public final synchronized DataTypeDescriptor[] getParams() throws StandardException {
        if (params == null && !lookedUpParams) {
            List tmpDefaults = new ArrayList();
            params = getDataDictionary().getSPSParams(this, tmpDefaults);
            paramDefaults = tmpDefaults.toArray();
            lookedUpParams = true;
        }

        return params;
    }

    /**
     * Set the list of parameters for this statement
     *
     * @param params the parameter list
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "DB-9289")
    public final synchronized void setParams(DataTypeDescriptor params[]) {
        this.params = params;
    }

    /**
     * Get the default parameter values for this statement.  Default parameter values are supplied by a USING clause on
     * either a CREATE or ALTER STATEMENT statement.
     *
     * @return the default parameter values
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9289")
    public final synchronized Object[] getParameterDefaults() throws StandardException {
        if (paramDefaults == null)
            getParams();
        return paramDefaults;
    }

    /**
     * Set the parameter defaults for this statement.
     *
     * @param values the parameter defaults
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "DB-9289")
    public final synchronized void setParameterDefaults(Object[] values) {
        this.paramDefaults = values;
    }

    /**
     * Get the preparedStatement for this statement.
     * If stmt is invalid or hasn't been compiled yet,
     * it will be recompiled.
     *
     * @return the preparedStatement
     */
    public final ExecPreparedStatement getPreparedStatement() throws StandardException {
        return getPreparedStatement(true);
    }

    /**
     * Get the preparedStatement for this statement. Expects the prepared statement to have already been added to
     * SYS.SYSSTATEMENTS.
     * <p/>
     * Side Effects: will update SYS.SYSSTATEMENTS with the new plan if it needs to be recompiled.
     *
     * @param recompIfInvalid if false, never recompile even
     *                        if statement is invalid
     */
    public final synchronized ExecPreparedStatement getPreparedStatement(boolean recompIfInvalid) throws StandardException {

        /*  Recompile if we are invalid, we don't have a prepared statement, or the statements activation
         *  has been cleared and cannot be reconstituted.*/
        if (recompIfInvalid && (!valid || (preparedStatement == null))) {
            ContextManager cm = ContextService.getFactory().getCurrentContextManager();

            /* Find the language connection context.  Get it each time in case a connection is dropped. */
            LanguageConnectionContext lcc = (LanguageConnectionContext)
                    cm.getContext(LanguageConnectionContext.CONTEXT_ID);

            if (!lcc.getDataDictionary().isReadOnlyUpgrade()) {

                // First try compiling in a nested transaction so we can
                // release the locks after the compilation, and not have them
                // sit around in the parent transaction. But if we get lock 
                // time out in the nested transaction, then go ahead and do 
                // the compilation in the user transaction. When doing the 
                // compilation in the user transaction, the locks acquired for 
                // recompilation will be released at the end of the user 
                // transaction (commit or abort).
                TransactionController nestedTC;
                try {
                    nestedTC = lcc.getTransactionCompile().startNestedUserTransaction(false, true);

                    // DERBY-3693: The nested transaction may run into a lock
                    // conflict with its parent transaction, in which case we
                    // don't want to wait for a timeout. If a lock timeout is
                    // detected while we're executing the nested transaction,
                    // we ignore the error and retry in the user transaction.
                    // When retrying in the user transaction, we'll wait for
                    // locks if necessary.
                    nestedTC.setNoLockWait(true);
                } catch (StandardException se) {
                    // If I cannot start a Nested User Transaction use the
                    // parent transaction to do all the work.
                    nestedTC = null;
                }

                // DERBY-2584: If the first attempt to compile the query fails,
                // we need to reset initiallyCompilable to make sure the
                // prepared plan is fully stored to disk. Save the initial
                // value here.
                final boolean compilable = initiallyCompilable;

                try {
                    prepareAndRelease(lcc, null, nestedTC);
                    updateSYSSTATEMENTS(lcc, RECOMPILE, nestedTC);
                } catch (StandardException se) {
                    if (se.isLockTimeout()) {
                        // Locks were set nowait, so a lock timeout here
                        // means that some lock request in the nested 
                        // transaction immediately conflicted.  A conflict
                        // with a parent lock would lead to a undetected 
                        // deadlock so must give up trying in the nested
                        // transaction and retry with parent transaction.
                        if (nestedTC != null) {
                            nestedTC.commit();
                            nestedTC.destroy();
                            nestedTC = null;
                        }

                        // if we couldn't do this with a nested transaction,
                        // retry with parent-- we need to wait this time!
                        // Lock conflicts at this point are with other 
                        // transactions, so must wait.
                        initiallyCompilable = compilable;
                        prepareAndRelease(lcc, null, null);
                        updateSYSSTATEMENTS(lcc, RECOMPILE, null);
                    } else {
                        // An example of a StandardException that can occur here is an exception indicating that
                        // the statement cannot be compiled because it is no longer valid.  Trigger actions that
                        // reference a column that now has a different incompatible type, for example. StandardException
                        // like this we should just throw.  WriteWrite conflicts (SE014) we ignore.  Ignoring WriteWrite
                        // here was added as part of DB-3386.  This is probably the behavoir we want sometimes
                        // (another transaction compiled the SPS, ok) but it is not clear that this will work for all
                        // cases. Probably some more work to do here.
                        if(!"SE014".equalsIgnoreCase(se.getSQLState())) {
                            throw se;
                        }
                    }
                } finally {
                    // no matter what, commit the nested transaction;
                    // if something bad happened in the child transaction lets
                    // not abort the parent here.
                    if (nestedTC != null) {
                        nestedTC.commit();
                        nestedTC.destroy();
                    }
                }
            }
        }

        return preparedStatement;
    }

    /**
     * Get the compilation type schema id when this view was first bound.
     */
    public final UUID getCompSchemaId() {
        return compSchemaId;
    }

    /**
     * Prints the contents of the TableDescriptor
     */
    @Override
    public final String toString() {
        if (SanityManager.DEBUG) {
            return "SPSDescriptor:\n" +
                    "\tname: " + sd.getSchemaName() + "." + name + "\n" +
                    "\tuuid: " + uuid + "\n" +
                    "\ttext: " + text + "\n" +
                    "\tvalid: " + ((valid) ? "TRUE" : "FALSE") + "\n" +
                    "\tpreparedStatement: " + preparedStatement + "\n";
        } else {
            return "";
        }
    }

    //////////////////////////////////////////////////////
    //
    // PROVIDER INTERFACE
    //
    //////////////////////////////////////////////////////

    /**
     * Return the stored form of this provider
     *
     * @see Dependable#getDependableFinder
     */
    @Override
    public final DependableFinder getDependableFinder() {
        return getDependableFinder(StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID);
    }

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String    The name of this provider.
     */
    @Override
    public final String getObjectName() {
        return name;
    }

    /**
     * Get the provider's UUID
     */
    @Override
    public final UUID getObjectID() {
        return uuid;
    }

    /**
     * Get the provider's type.
     */
    @Override
    public final String getClassType() {
        return Dependable.STORED_PREPARED_STATEMENT;
    }

    //////////////////////////////////////////////////////
    //
    // DEPENDENT INTERFACE
    //
    //////////////////////////////////////////////////////

    /**
     * Check that all of the dependent's dependencies are valid.
     */
    @Override
    public final synchronized boolean isValid() {
        return valid;
    }

    /**
     * Prepare to mark the dependent as invalid (due to at least one of
     * its dependencies being invalid).
     *
     * @param action The action causing the invalidation
     * @param p      the provider
     * @throws StandardException thrown if unable to make it invalid
     */
    @Override
    public final void prepareToInvalidate(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {
        switch (action) {
            /*
            ** Things that don't affect us
            */
            case DependencyManager.CREATE_VIEW:

            /*
            ** Things that force a recompile, but are
            ** allowed.
            */
            case DependencyManager.CREATE_INDEX:
            case DependencyManager.CREATE_CONSTRAINT:
            case DependencyManager.DROP_CONSTRAINT:
            case DependencyManager.DROP_INDEX:
            case DependencyManager.DROP_TABLE:
            case DependencyManager.DROP_VIEW:
            case DependencyManager.DROP_METHOD_ALIAS:
            case DependencyManager.DROP_SYNONYM:
            case DependencyManager.ALTER_TABLE:
            case DependencyManager.RENAME:
            case DependencyManager.RENAME_INDEX:
            case DependencyManager.PREPARED_STATEMENT_RELEASE:
            case DependencyManager.USER_RECOMPILE_REQUEST:
            case DependencyManager.CHANGED_CURSOR:
            case DependencyManager.BULK_INSERT:
            case DependencyManager.COMPRESS_TABLE:
            case DependencyManager.SET_CONSTRAINTS_ENABLE:
            case DependencyManager.SET_CONSTRAINTS_DISABLE:
            case DependencyManager.SET_TRIGGERS_ENABLE:
            case DependencyManager.SET_TRIGGERS_DISABLE:
            case DependencyManager.ROLLBACK:
            case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
            case DependencyManager.CREATE_TRIGGER:
            case DependencyManager.DROP_TRIGGER:
            case DependencyManager.DROP_COLUMN:
            case DependencyManager.DROP_COLUMN_RESTRICT:
            case DependencyManager.UPDATE_STATISTICS:
            case DependencyManager.DROP_STATISTICS:
            case DependencyManager.TRUNCATE_TABLE:
                break;

            /*
            ** The rest are errors
            */
            default:

                DependencyManager dm;

                dm = getDataDictionary().getDependencyManager();
                throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_S_P_S,
                        dm.getActionString(action),
                        p.getObjectName(), name);

        }
    }

    /**
     * Mark the dependent as invalid (due to at least one of
     * its dependencies being invalid).
     *
     * @param action The action causing the invalidation
     * @throws StandardException thrown if unable to make it invalid
     */
    @Override
    public final synchronized void makeInvalid(int action, LanguageConnectionContext lcc) throws StandardException {
        DependencyManager dm = getDataDictionary().getDependencyManager();

        switch (action) {
            /*
            ** Some things that don't affect stored prepared
             ** statements.
            */
            case DependencyManager.PREPARED_STATEMENT_RELEASE:
            case DependencyManager.CREATE_VIEW:
                break;

            /*
             ** Things that can invalidate a stored
            ** prepared statement.
            */
            case DependencyManager.CREATE_INDEX:
            case DependencyManager.CREATE_CONSTRAINT:
            case DependencyManager.DROP_CONSTRAINT:
            case DependencyManager.DROP_TABLE:
            case DependencyManager.DROP_INDEX:
            case DependencyManager.DROP_VIEW:
            case DependencyManager.DROP_METHOD_ALIAS:
            case DependencyManager.DROP_SYNONYM:
            case DependencyManager.ALTER_TABLE:
            case DependencyManager.RENAME:
            case DependencyManager.RENAME_INDEX:
            case DependencyManager.USER_RECOMPILE_REQUEST:
            case DependencyManager.CHANGED_CURSOR:
            case DependencyManager.BULK_INSERT:
            case DependencyManager.COMPRESS_TABLE:
            case DependencyManager.SET_CONSTRAINTS_ENABLE:
            case DependencyManager.SET_CONSTRAINTS_DISABLE:
            case DependencyManager.SET_TRIGGERS_ENABLE:
            case DependencyManager.SET_TRIGGERS_DISABLE:
            case DependencyManager.ROLLBACK:
            case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
            case DependencyManager.CREATE_TRIGGER:
            case DependencyManager.DROP_TRIGGER:
            case DependencyManager.DROP_COLUMN:
            case DependencyManager.DROP_COLUMN_RESTRICT:
            case DependencyManager.UPDATE_STATISTICS:
            case DependencyManager.DROP_STATISTICS:
            case DependencyManager.TRUNCATE_TABLE:
                /*
                ** If we are already invalid, don't write ourselves
                ** out.  Just to be safe, we'll send out an invalidate
                ** to our dependents either way.
                */
                if (valid) {
                    valid = false;
                    preparedStatement = null;
                    updateSYSSTATEMENTS(lcc, INVALIDATE, null);
                }
                dm.invalidateFor(this, DependencyManager.USER_RECOMPILE_REQUEST, lcc);
                break;
            case DependencyManager.DROP_SPS:
                //System.out.println("SPSD " + preparedStatement);
                dm.clearDependencies(lcc, this);
                break;

            default:

                /* 
                ** We should never get here, since we can't have dangling references 
                */
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("makeInvalid(" +
                            dm.getActionString(action) +
                            ") not expected to get called; should have failed in " +
                            "prepareToInvalidate()");
                }
                break;

        }

    }

    /**
     * Invalidate and revalidate.  The functional equivalent
     * of calling makeInvalid() and makeValid(), except it
     * is optimized.
     */
    public final synchronized void revalidate(LanguageConnectionContext lcc) throws StandardException {
        /* Mark it as invalid first to ensure that we don't write SYSSTATEMENTS 2x. */
        valid = false;
        makeInvalid(DependencyManager.USER_RECOMPILE_REQUEST, lcc);
        prepareAndRelease(lcc);
        updateSYSSTATEMENTS(lcc, RECOMPILE, null);
    }

    /**
     * Load the underlying generatd class.  This is not expected
     * to be used outside of the datadictionary package.  It
     * is used for optimizing class loading for sps
     * cacheing.
     */
    public void loadGeneratedClass() throws StandardException {
        /*
        ** On upgrade, we null out the statement body,
        ** so handle that here.
        */
        if (preparedStatement != null) {
            ((StorablePreparedStatement) preparedStatement).loadGeneratedClass();
        }
    }

    /*
    ** Update SYSSTATEMENTS with the changed the descriptor.
    ** Always done in the user XACT.
    ** <p>
    ** Ideally, the changes to SYSSTATEMENTS would be made
    ** in a separate xact as the current user xact, but this
    ** is painful (you'ld need to get a new ContextManager
    ** and then push all of the usual langauge contexts
    ** onto it and THEN call AccessManager.getTransaction()),
    ** and it wont work, because the xact is in a different
    ** compatibility space and will self deadlock (e.g.
    ** in the process of call DependencyManager.makeInvalid()
    ** we first did a DDdependableFinder.getDependable()
    ** which called DataDictionaryImpl.getSPSDescriptor()
    ** so we hold a lock on SYS.SYSSTATEMENTS by the
    ** time we get a 2nd xact and try to drop the statement).
    */
    private void updateSYSSTATEMENTS(LanguageConnectionContext lcc, int mode, TransactionController tc)
            throws StandardException {
        boolean updateSYSCOLUMNS, recompile;
        boolean firstCompilation = false;
        if (mode == RECOMPILE) {
            recompile = true;
            updateSYSCOLUMNS = true;
            if (!initiallyCompilable) {
                firstCompilation = true;
                initiallyCompilable = true;
            }
        } else {
            recompile = false;
            updateSYSCOLUMNS = false;
        }

        DataDictionary dd = getDataDictionary();

        String role = lcc.getReplicationRole();
        if (dd.isReadOnlyUpgrade() || role.compareToIgnoreCase("REPLICA") == 0 ||
                lcc.getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.SNAPSHOT_TIMESTAMP) != null)
            return;


        /*
        ** Get busy time
        */
        dd.startWriting(lcc);

        if (tc == null) { //bug 4821 - tc will passed null if we want to use the user transaction
            tc = lcc.getTransactionExecute();
        }
        try {
            dd.updateSPS(this,
                    tc,
                    recompile,
                    updateSYSCOLUMNS,
                    firstCompilation);

        } catch (StandardException se) {
            // This can fail since multiple threads/clusters can recompile sps descriptors
            // especially after collecting stats on sys tables.

        }
    }

    /**
     * Get the UUID for the given string
     *
     * @param idString the string
     * @return the UUID
     */
    private UUID recreateUUID(String idString) {
        if (uuidFactory == null) {
            uuidFactory = Monitor.getMonitor().getUUIDFactory();
        }
        return uuidFactory.recreateUUID(idString);
    }

    /**
     * @see TupleDescriptor#getDescriptorType
     */
    @Override
    public String getDescriptorType() {
        return "Statement";
    }

    // RESOLVE: some descriptors have getName.  some descriptors have
    // getTableName, getColumnName whatever! try and unify all of this to one
    // getDescriptorName!
    @Override
    public String getDescriptorName() {
        return name;
    }

}

