/*

   Derby - Class org.apache.derby.impl.sql.compile.DefaultVTIModDeferPolicy

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.vti.DeferModification;

/**
 * This class implements the default policy for defering modifications to virtual
 * tables.
 */
class DefaultVTIModDeferPolicy implements DeferModification
{
    private final String targetVTIClassName;
    private final boolean VTIResultSetIsSensitive;

    DefaultVTIModDeferPolicy( String targetVTIClassName,
                              boolean VTIResultSetIsSensitive)
    {
        this.targetVTIClassName = targetVTIClassName;
        this.VTIResultSetIsSensitive = VTIResultSetIsSensitive;
    }

    /**
     * @see com.splicemachine.db.vti.DeferModification#alwaysDefer
     */
    public boolean alwaysDefer( int statementType)
    {
        return false;
    }
          
    /**
     * @see com.splicemachine.db.vti.DeferModification#columnRequiresDefer
     */
    public boolean columnRequiresDefer( int statementType,
                                        String columnName,
                                        boolean inWhereClause)
    {
        switch( statementType)
        {
        case DeferModification.INSERT_STATEMENT:
            return false;

        case DeferModification.UPDATE_STATEMENT:
            return VTIResultSetIsSensitive && inWhereClause;

        case DeferModification.DELETE_STATEMENT:
            return false;
        }
        return false; // Should not get here.
    } // end of columnRequiresDefer

    /**
     * @see com.splicemachine.db.vti.DeferModification#subselectRequiresDefer(int,String,String)
     */
    public boolean subselectRequiresDefer( int statementType,
                                           String schemaName,
                                           String tableName)
    {
        return false;
    } // end of subselectRequiresDefer( statementType, schemaName, tableName)

    /**
     * @see com.splicemachine.db.vti.DeferModification#subselectRequiresDefer(int, String)
     */
    public boolean subselectRequiresDefer( int statementType,
                                           String VTIClassName)
    {
        return targetVTIClassName.equals( VTIClassName);
    } // end of subselectRequiresDefer( statementType, VTIClassName)

    public void modificationNotify( int statementType,
                                    boolean deferred)
    {}
}
