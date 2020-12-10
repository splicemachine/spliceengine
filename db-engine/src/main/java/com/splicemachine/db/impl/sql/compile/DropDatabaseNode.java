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
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.dictionary.DatabaseDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * A DropDatabaseNode is the root of a QueryTree that represents
 * a DROP DATABASE statement.
 *
 */

public class DropDatabaseNode extends DDLStatementNode
{
    private int dropBehavior;
    private String dbName;

    /**
     * Initializer for a DropDatabaseNode
     *
     * @param dbName The name of the object being dropped
     * @param dropBehavior Drop behavior (RESTRICT | CASCADE)
     *
     */
    public void init(Object dbName, Object dropBehavior)
        throws StandardException
    {
        initAndCheck(null);
        this.dbName = (String) dbName;
        this.dropBehavior = (Integer) dropBehavior;
    }

    public void bindStatement() throws StandardException
    {
        /*
        ** Users are not permitted to drop
        ** the SYS or SPLICE schemas.
        */
        if (dbName.equals(DatabaseDescriptor.STD_DB_NAME)) {
            throw(StandardException.newException(
                    SQLState.LANG_CANNOT_DROP_DEFAULT_DB, dbName));
        }

        // XXX (arnaud multidb check privilege collection required (see drop schema node)
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */
    public String toString()
    {
        if (SanityManager.DEBUG) {
            return super.toString() + "dropBehavior: " + "\n" + dropBehavior + "\n";
        } else {
            return "";
        }
    }

    public String statementToString()
    {
        return "DROP DATABASE";
    }

    // inherit generate() method from DDLStatementNode


    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException        Thrown on failure
     */
    public ConstantAction    makeConstantAction() throws StandardException
    {
        return    getGenericConstantActionFactory().getDropDatabaseConstantAction(dbName);
    }
}
