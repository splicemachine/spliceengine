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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.PrivilegedSQLObject;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.impl.sql.execute.GenericPrivilegeInfo;
import com.splicemachine.db.impl.sql.execute.PrivilegeInfo;
import com.splicemachine.db.catalog.TypeDescriptor;

import java.util.HashMap;
import java.util.List;

/**
 * This node represents a set of privileges that are granted or revoked on one object.
 */
public class PrivilegeNode extends QueryTreeNode
{
    // Privilege object type
    public static final int TABLE_PRIVILEGES = 0;
    public static final int ROUTINE_PRIVILEGES = 1;
    public static final int SEQUENCE_PRIVILEGES = 2;
    public static final int UDT_PRIVILEGES = 3;
    public static final int AGGREGATE_PRIVILEGES = 4;
    public static final int SCHEMA_PRIVILEGES = 5;
    //
    // State initialized when the node is instantiated
    //
    private int objectType;
    private TableName objectName;
    private String schemaName;
    private BasicPrivilegesNode specificPrivileges; // Null for routine and usage privs
    private RoutineDesignator routineDesignator; // null for table and usage privs

    private String privilege;  // E.g., PermDescriptor.USAGE_PRIV
    private boolean restrict;

    //
    // State which is filled in by the bind() logic.
    //
    private Provider dependencyProvider;
    
    /**
     * Initialize a PrivilegeNode for use against SYS.SYSTABLEPERMS and SYS.SYSROUTINEPERMS.
     *
     * @param objectType (an Integer)
     * @param objectOfPrivilege (a TableName or RoutineDesignator)
     * @param specificPrivileges null for routines and usage
     */
    public void init( Object objectType, Object objectOfPrivilege, Object specificPrivileges)
        throws StandardException
    {
        this.objectType = ((Integer) objectType).intValue();
        if( SanityManager.DEBUG)
        {
            SanityManager.ASSERT( objectOfPrivilege != null,
                                  "null privilge object");
        }
        switch( this.objectType)
        {
        case TABLE_PRIVILEGES:
            if( SanityManager.DEBUG)
            {
                SanityManager.ASSERT( specificPrivileges != null,
                                      "null specific privileges used with table privilege");
            }
            objectName = (TableName) objectOfPrivilege;
            this.specificPrivileges = (BasicPrivilegesNode) specificPrivileges;
            break;
        case SCHEMA_PRIVILEGES:
            if( SanityManager.DEBUG)
            {
                SanityManager.ASSERT( specificPrivileges != null,
                        "null specific privileges used with schema privilege");
            }
            schemaName = (String) objectOfPrivilege;
            this.specificPrivileges = (BasicPrivilegesNode) specificPrivileges;
        break;
        case ROUTINE_PRIVILEGES:
            if( SanityManager.DEBUG)
            {
                SanityManager.ASSERT( specificPrivileges == null,
                                      "non-null specific privileges used with execute privilege");
            }
            routineDesignator = (RoutineDesignator) objectOfPrivilege;
            objectName = routineDesignator.name;
            break;
            
        default:
            throw unimplementedFeature();
        }
    }

    /**
     * Initialize a PrivilegeNode for use against SYS.SYSPERMS.
     *
     * @param objectType E.g., SEQUENCE
     * @param objectName A possibles schema-qualified name
     * @param privilege A PermDescriptor privilege, e.g. PermDescriptor.USAGE_PRIV
     * @param restrict True if this is a REVOKE...RESTRICT action
     */
    public void init( Object objectType, Object objectName, Object privilege, Object restrict )
    {
        this.objectType = ((Integer) objectType).intValue();
        this.objectName = (TableName) objectName;
        this.privilege = (String) privilege;
        this.restrict = ((Boolean) restrict).booleanValue();
    }

    /**
     * Utility function, make sure we don't revoke ourself from the list of grantees.
     * @param sd
     * @param grantees
     * @throws StandardException
     */
    public void verifySelfGrantRevoke(SchemaDescriptor sd, List grantees) throws StandardException{


        // Can not grant/revoke permissions from self
        if (grantees.contains(sd.getAuthorizationId()))
        {
            throw StandardException.newException
                    (SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, sd.getObjectName());
        }
    }
    
    /**
     * Bind this GrantNode. Resolve all table, column, and routine references. Register
     * a dependency on the object of the privilege if it has not already been done
     *
     * @param dependencies The list of privilege objects that this statement has already seen.
     *               If the object of this privilege is not in the list then this statement is registered
     *               as dependent on the object.
     * @param grantees The list of grantees
     * @param isGrant grant if true; revoke if false
     * @return the bound node
     *
     * @exception StandardException	Standard error policy.
     */
	public QueryTreeNode bind( HashMap dependencies, List grantees, boolean isGrant ) throws StandardException
	{
        SchemaDescriptor sd;

        switch( objectType)
        {
        case SCHEMA_PRIVILEGES:
            sd = getSchemaDescriptor( schemaName, true);
            verifySelfGrantRevoke(sd,grantees);

            if (sd.isSystemSchema())
            {
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
            }

            // Don't allow authorization on SESSION schema tables. Causes confusion if
            // a temporary table is created later with same name.
            if (isSessionSchema(sd.getSchemaName()))
            {
                throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
            }
            specificPrivileges.bind( sd, isGrant);
            dependencyProvider = sd;
            break;
        case TABLE_PRIVILEGES:

            sd = getSchemaDescriptor( objectName.getSchemaName(), true);
            verifySelfGrantRevoke(sd,grantees);

            // The below code handles the case where objectName.getSchemaName()
            // returns null, in which case we'll fetch the schema descriptor for
            // the current compilation schema (see getSchemaDescriptor).
            objectName.setSchemaName( sd.getSchemaName() );
            // can't grant/revoke privileges on system tables
            if (sd.isSystemSchema())
            {
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
            }
            
            TableDescriptor td = getTableDescriptor( objectName.getTableName(), sd);
            if( td == null)
            {
                throw StandardException.newException( SQLState.LANG_TABLE_NOT_FOUND, objectName.toString());
            }

            // Don't allow authorization on SESSION schema tables. Causes confusion if
            // a temporary table is created later with same name.
            if (isSessionSchema(sd.getSchemaName()))
            {
                throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
            }

            if (td.getTableType() != TableDescriptor.BASE_TABLE_TYPE &&
            		td.getTableType() != TableDescriptor.VIEW_TYPE)
            {
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
            }

            specificPrivileges.bind( td, isGrant);
            dependencyProvider = td;
            break;

        case ROUTINE_PRIVILEGES:
            sd = getSchemaDescriptor( objectName.getSchemaName(), true);
            verifySelfGrantRevoke(sd,grantees);

            // The below code handles the case where objectName.getSchemaName()
            // returns null, in which case we'll fetch the schema descriptor for
            // the current compilation schema (see getSchemaDescriptor).
            objectName.setSchemaName( sd.getSchemaName() );

            if (!sd.isSchemaWithGrantableRoutines())
            {
                throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED, objectName.getFullTableName());
            }
				
            AliasDescriptor proc = null;
            RoutineAliasInfo routineInfo;
            List<AliasDescriptor> list = getDataDictionary().getRoutineList(
                sd.getUUID().toString(), objectName.getTableName(),
                routineDesignator.isFunction ? AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR : AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR
                );

            if( routineDesignator.paramTypeList == null)
            {
                // No signature was specified. Make sure that there is exactly one routine with that name.
                if( list.size() > 1)
                {
                    throw StandardException.newException( ( routineDesignator.isFunction ? SQLState.LANG_AMBIGUOUS_FUNCTION_NAME
                                                            : SQLState.LANG_AMBIGUOUS_PROCEDURE_NAME),
                                                          objectName.getFullTableName());
                }
                if( list.size() != 1) {
                    if (routineDesignator.isFunction) {
                        throw StandardException.newException(SQLState.LANG_NO_SUCH_FUNCTION, 
                                objectName.getFullTableName());
                    } else {
                        throw StandardException.newException(SQLState.LANG_NO_SUCH_PROCEDURE, 
                                objectName.getFullTableName());
                    }
                }
                proc = (AliasDescriptor) list.get(0);
            }
            else
            {
                // The full signature was specified
                boolean found = false;
                for (int i = list.size() - 1; (!found) && i >= 0; i--)
                {
                    proc = (AliasDescriptor) list.get(i);

                    routineInfo = (RoutineAliasInfo) proc.getAliasInfo();
                    int parameterCount = routineInfo.getParameterCount();
                    if (parameterCount != routineDesignator.paramTypeList.size())
                        continue;
                    TypeDescriptor[] parameterTypes = routineInfo.getParameterTypes();
                    found = true;
                    for( int parmIdx = 0; parmIdx < parameterCount; parmIdx++)
                    {
                        if( ! parameterTypes[parmIdx].equals( routineDesignator.paramTypeList.get( parmIdx)))
                        {
                            found = false;
                            break;
                        }
                    }
                }
                if( ! found)
                {
                    // reconstruct the signature for the error message
                    StringBuffer sb = new StringBuffer( objectName.getFullTableName());
                    sb.append( "(");
                    for( int i = 0; i < routineDesignator.paramTypeList.size(); i++)
                    {
                        if( i > 0)
                            sb.append(",");
                        sb.append( routineDesignator.paramTypeList.get(i).toString());
                    }
                    throw StandardException.newException(SQLState.LANG_NO_SUCH_METHOD_ALIAS, sb.toString());
                }
            }
            routineDesignator.setAliasDescriptor( proc);
            dependencyProvider = proc;
            break;
        case AGGREGATE_PRIVILEGES:

            sd = getSchemaDescriptor( objectName.getSchemaName(), true);
            verifySelfGrantRevoke(sd,grantees);

            // The below code handles the case where objectName.getSchemaName()
            // returns null, in which case we'll fetch the schema descriptor for
            // the current compilation schema (see getSchemaDescriptor).
            objectName.setSchemaName( sd.getSchemaName() );
                   
            dependencyProvider = getDataDictionary().getAliasDescriptor
            ( sd.getUUID().toString(), objectName.getTableName(), AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR  );
            if ( dependencyProvider == null )
            {
                 throw StandardException.newException
                 (SQLState.LANG_OBJECT_NOT_FOUND, "DERBY AGGREGATE", objectName.getFullTableName());
            }
            break;           

        case SEQUENCE_PRIVILEGES:

            sd = getSchemaDescriptor( objectName.getSchemaName(), true);
            verifySelfGrantRevoke(sd,grantees);

            // The below code handles the case where objectName.getSchemaName()
            // returns null, in which case we'll fetch the schema descriptor for
            // the current compilation schema (see getSchemaDescriptor).
            objectName.setSchemaName( sd.getSchemaName() );
            
            dependencyProvider = getDataDictionary().getSequenceDescriptor( sd, objectName.getTableName() );
            if ( dependencyProvider == null )
            {
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "SEQUENCE", objectName.getFullTableName());
            }
            break;
            
        case UDT_PRIVILEGES:

            sd = getSchemaDescriptor( objectName.getSchemaName(), true);
            verifySelfGrantRevoke(sd,grantees);

            // The below code handles the case where objectName.getSchemaName()
            // returns null, in which case we'll fetch the schema descriptor for
            // the current compilation schema (see getSchemaDescriptor).
            objectName.setSchemaName( sd.getSchemaName() );
            
            dependencyProvider = getDataDictionary().getAliasDescriptor
                ( sd.getUUID().toString(), objectName.getTableName(), AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR  );
            if ( dependencyProvider == null )
            {
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "TYPE", objectName.getFullTableName());
            }
            break;
            
        default:
            throw unimplementedFeature();
        }

        if( dependencyProvider != null)
        {
            if( dependencies.get( dependencyProvider) == null)
            {
                getCompilerContext().createDependency( dependencyProvider);
                dependencies.put( dependencyProvider, dependencyProvider);
            }
        }
        return this;
    } // end of bind


    /**
     * @return PrivilegeInfo for this node
     */
    PrivilegeInfo makePrivilegeInfo() throws StandardException
    {
        switch( objectType)
        {
        case TABLE_PRIVILEGES:
            return specificPrivileges.makePrivilegeInfo();
        case SCHEMA_PRIVILEGES:
                return specificPrivileges.makeSchemaPrivilegeInfo();
        case ROUTINE_PRIVILEGES:
            return routineDesignator.makePrivilegeInfo();
        case AGGREGATE_PRIVILEGES:
        case SEQUENCE_PRIVILEGES:
        case UDT_PRIVILEGES:
            return new GenericPrivilegeInfo( (PrivilegedSQLObject) dependencyProvider, privilege, restrict );

        default:
            throw unimplementedFeature();
        }
    }

    /** Report an unimplemented feature */
    private StandardException unimplementedFeature()
    {
        return StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
    }
}
