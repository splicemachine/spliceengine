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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.DefaultDescriptor;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *	Class for most DependableFinders in the core DataDictionary.
 * This class is stored in SYSDEPENDS for the finders for
 * the provider and dependent. It stores no state, its functionality
 * is driven off its format identifier.
 *
 *
 */

public class DDdependableFinder implements	DependableFinder, Formatable
{
	private static final long serialVersionUID = 1L;
	////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	////////////////////////////////////////////////////////////////////////

	private int formatId;

	////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	////////////////////////////////////////////////////////////////////////

	/**
 	  * Serialization Constructor. DO NOT USER
	  */
	public DDdependableFinder()
	{

	}

	/**
	  *	Public constructor for Formatable hoo-hah.
	  */
	public	DDdependableFinder(int formatId)
	{
		this.formatId = formatId;
	}

	//////////////////////////////////////////////////////////////////
	//
	//	OBJECT SUPPORT
	//
	//////////////////////////////////////////////////////////////////

	public	String	toString()
	{
		return	getSQLObjectType();
	}

	//////////////////////////////////////////////////////////////////
	//
	//	VACUOUS FORMATABLE INTERFACE. ALL THAT A VACUOUSDEPENDABLEFINDER
	//	NEEDS TO DO IS STAMP ITS FORMAT ID ONTO THE OUTPUT STREAM.
	//
	//////////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects. Nothing to
	 * do. Our persistent representation is just a 2-byte format id.
	 *
	 * @param in read this.
	 */
    public void readExternal( ObjectInput in )
			throws IOException, ClassNotFoundException
	{
		formatId = in.readInt();
	}

	/**
	 * Write this object to a stream of stored objects. Again, nothing
	 * to do. We just stamp the output stream with our Format id.
	 *
	 * @param out write bytes here.
	 */
    public void writeExternal( ObjectOutput out )
			throws IOException
	{
		out.writeInt(formatId);
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	final int	getTypeFormatId()	
	{
		return formatId;
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	DDdependable METHODS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  * @see DependableFinder#getSQLObjectType
	  */
	public	String	getSQLObjectType()
	{
		switch (formatId)
		{
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.ALIAS;

			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.CONGLOMERATE;

			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.CONSTRAINT;

			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.DEFAULT;

			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
				return Dependable.FILE;

			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.SCHEMA;

			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.STORED_PREPARED_STATEMENT;

			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.TABLE;

			case StoredFormatIds.COLUMN_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.COLUMNS_IN_TABLE;

			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.TRIGGER;

			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.VIEW;

			case StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID:
				return Dependable.TABLE_PERMISSION;

			case StoredFormatIds.SCHEMA_PERMISSION_FINDER_V01_ID:
				return Dependable.SCHEMA_PERMISSION;

			case StoredFormatIds.COLUMNS_PERMISSION_FINDER_V01_ID:
				return Dependable.COLUMNS_PERMISSION;

			case StoredFormatIds.ROUTINE_PERMISSION_FINDER_V01_ID:
				return Dependable.ROUTINE_PERMISSION;

			case StoredFormatIds.ROLE_GRANT_FINDER_V01_ID:
				return Dependable.ROLE_GRANT;

			case StoredFormatIds.SEQUENCE_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.SEQUENCE;

			case StoredFormatIds.PERM_DESCRIPTOR_FINDER_V01_ID:
				return Dependable.PERM;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"getSQLObjectType() called with unexpeced formatId = " + formatId);
				}
				return null;
		}
	}

	/**
		Get the dependable for the given UUID
		@exception StandardException thrown on error
	*/
	public final Dependable getDependable(DataDictionary dd, UUID dependableObjectID)
		throws StandardException
	{
        Dependable dependable = findDependable(dd, dependableObjectID);
        if (dependable == null)
            throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND,
                    getSQLObjectType(), dependableObjectID);
        return dependable;
    }
        
       
    /**
     * Find the dependable for getDependable.
     * Can return a null references, in which case getDependable()
     * will thrown an exception.
     */
    Dependable findDependable(DataDictionary dd, UUID dependableObjectID)
        throws StandardException
    {     
		switch (formatId)
		{
			case StoredFormatIds.ALIAS_DESCRIPTOR_FINDER_V01_ID:
                return dd.getAliasDescriptor(dependableObjectID);

			case StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID:
                return dd.getConglomerateDescriptor(dependableObjectID);

			case StoredFormatIds.CONSTRAINT_DESCRIPTOR_FINDER_V01_ID:
                return dd.getConstraintDescriptor(dependableObjectID);

			case StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID:
				ColumnDescriptor	cd = dd.getColumnDescriptorByDefaultId(dependableObjectID);
                if (cd != null)
                    return new DefaultDescriptor(
												dd, 
												cd.getDefaultUUID(), cd.getReferencingUUID(), 
												cd.getPosition());
                return null;

			case StoredFormatIds.FILE_INFO_FINDER_V01_ID:
                return dd.getFileInfoDescriptor(dependableObjectID);

			case StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID:
                return dd.getSchemaDescriptor(dependableObjectID, null);

			case StoredFormatIds.SPS_DESCRIPTOR_FINDER_V01_ID:
                return dd.getSPSDescriptor(dependableObjectID);

			case StoredFormatIds.TABLE_DESCRIPTOR_FINDER_V01_ID:
                return dd.getTableDescriptor(dependableObjectID);

			case StoredFormatIds.TRIGGER_DESCRIPTOR_FINDER_V01_ID:
                return dd.getTriggerDescriptor(dependableObjectID);
 
			case StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID:
                return dd.getViewDescriptor(dependableObjectID);

            case StoredFormatIds.COLUMNS_PERMISSION_FINDER_V01_ID:
                return dd.getColumnPermissions(dependableObjectID);

			case StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID:
                return dd.getTablePermissions(dependableObjectID);

			case StoredFormatIds.SCHEMA_PERMISSION_FINDER_V01_ID:
				return dd.getSchemaPermissions(dependableObjectID);

			case StoredFormatIds.ROUTINE_PERMISSION_FINDER_V01_ID:
                return dd.getRoutinePermissions(dependableObjectID);

		    case StoredFormatIds.ROLE_GRANT_FINDER_V01_ID:
				return dd.getRoleGrantDescriptor(dependableObjectID);

			case StoredFormatIds.SEQUENCE_DESCRIPTOR_FINDER_V01_ID:
                return dd.getSequenceDescriptor(dependableObjectID);

			case StoredFormatIds.PERM_DESCRIPTOR_FINDER_V01_ID:
                return dd.getGenericPermissions(dependableObjectID);

		default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"getDependable() called with unexpeced formatId = " + formatId);
				}
                return null;
		}
    }
}
