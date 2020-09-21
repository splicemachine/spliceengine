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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * A LockTableNode is the root of a QueryTree that represents a LOCK TABLE command:
 *    LOCK TABLE <TableName> IN SHARE/EXCLUSIVE MODE
 *
 */

public class LockTableNode extends MiscellaneousStatementNode
{
    private TableName    tableName;
    private boolean        exclusiveMode;
    private long        conglomerateNumber;
    private TableDescriptor            lockTableDescriptor;

    /**
     * Initializer for LockTableNode
     *
     * @param tableName        The table to lock
     * @param exclusiveMode    boolean, whether or not to get an exclusive lock.
     */
    public void init(Object tableName, Object exclusiveMode)
    {
        this.tableName = (TableName) tableName;
        this.exclusiveMode = (Boolean) exclusiveMode;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "tableName: " + tableName + "\n" +
                "exclusiveMode: " + exclusiveMode + "\n" +
                "conglomerateNumber: " + conglomerateNumber + "\n" +
                super.toString();
        }
        else
        {
            return "";
        }
    }

    public String statementToString()
    {
        return "LOCK TABLE";
    }

    /**
     * Bind this LockTableNode.  This means looking up the table,
     * verifying it exists and getting the heap conglomerate number.
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void bindStatement() throws StandardException
    {
        CompilerContext            cc = getCompilerContext();
        ConglomerateDescriptor    cd;
        SchemaDescriptor        sd;

        String schemaName = tableName.getSchemaName();
        sd = getSchemaDescriptor(schemaName);

        // Users are not allowed to lock system tables
        if (sd.isSystemSchema())
        {
            throw StandardException.newException(SQLState.LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA,
                            statementToString(), schemaName);
        }

        lockTableDescriptor = getTableDescriptor(tableName.getTableName(), sd);

        if (lockTableDescriptor == null)
        {
            // Check if the reference is for a synonym.
            TableName synonymTab = resolveTableToSynonym(tableName);
            if (synonymTab == null)
                throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, tableName.toString());
            tableName = synonymTab;
            sd = getSchemaDescriptor(tableName.getSchemaName());

            lockTableDescriptor = getTableDescriptor(synonymTab.getTableName(), sd);
            if (lockTableDescriptor == null)
                throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, tableName.toString());
        }

        //throw an exception if user is attempting to lock a temporary table
        if (lockTableDescriptor.getTableType() == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
        {
                throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);
        }

        conglomerateNumber = lockTableDescriptor.getHeapConglomerateId();

        /* Get the base conglomerate descriptor */
        cd = lockTableDescriptor.getConglomerateDescriptor(conglomerateNumber);

        /* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
        cc.createDependency(lockTableDescriptor);
        cc.createDependency(cd);

        if (isPrivilegeCollectionRequired())
        {
            // need SELECT privilege to perform lock table statement.
            cc.pushCurrentPrivType(Authorizer.SELECT_PRIV);
            cc.addRequiredTablePriv(lockTableDescriptor);
            cc.popCurrentPrivType();
        }
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @return    true if references SESSION schema tables, else false
     *
     * @exception StandardException        Thrown on error
     */
    public boolean referencesSessionSchema()
        throws StandardException
    {
        //If lock table is on a SESSION schema table, then return true.
        return isSessionSchema(lockTableDescriptor.getSchemaName());
    }

    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException        Thrown on failure
     */
    public ConstantAction    makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().getLockTableConstantAction(
                        tableName.getFullTableName(),
                        conglomerateNumber,
                        exclusiveMode);
    }
}
