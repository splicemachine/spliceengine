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
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;

/**
 * This interface is used to get information from a DefaultDescriptor.
 *
 */

public final class DefaultDescriptor 
	extends TupleDescriptor
	implements UniqueTupleDescriptor, Provider, Dependent
{
	private final int			columnNumber;
	private final UUID		defaultUUID;
	private final UUID		tableUUID;

	/**
	 * Constructor for a DefaultDescriptor
	 *
	 * @param dataDictionary    the DD
	 * @param defaultUUID		The UUID of the default
	 * @param tableUUID			The UUID of the table
	 * @param columnNumber		The column number of the column that the default is for
	 */

	public DefaultDescriptor(DataDictionary dataDictionary, UUID defaultUUID, UUID tableUUID, int columnNumber)
	{
		super( dataDictionary );

		this.defaultUUID = defaultUUID;
		this.tableUUID = tableUUID;
		this.columnNumber = columnNumber;
	}

	/**
	 * Get the UUID of the default.
	 *
	 * @return	The UUID of the default.
	 */
	public UUID	getUUID()
	{
		return defaultUUID;
	}

	/**
	 * Get the UUID of the table.
	 *
	 * @return	The UUID of the table.
	 */
	public UUID	getTableUUID()
	{
		return tableUUID;
	}

	/**
	 * Get the column number of the column.
	 *
	 * @return	The column number of the column.
	 */
	public int	getColumnNumber()
	{
		return columnNumber;
	}

	/**
	 * Convert the DefaultDescriptor to a String.
	 *
	 * @return	A String representation of this DefaultDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			/*
			** NOTE: This does not format table, because table.toString()
			** formats columns, leading to infinite recursion.
			*/
			return "defaultUUID: " + defaultUUID + "\n" +
				"tableUUID: " + tableUUID + "\n" +
				"columnNumber: " + columnNumber + "\n";
		}
		else
		{
			return "";
		}
	}

	////////////////////////////////////////////////////////////////////
	//
	// PROVIDER INTERFACE
	//
	////////////////////////////////////////////////////////////////////

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
	    return	getDependableFinder(StoredFormatIds.DEFAULT_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return "default";
	}

	/**
	 * Get the provider's UUID
	 *
	 * @return 	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return defaultUUID;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.DEFAULT;
	}

	//////////////////////////////////////////////////////
	//
	// DEPENDENT INTERFACE
	//
	//////////////////////////////////////////////////////
	/**
	 * Check that all of the dependent's dependencies are valid.
	 *
	 * @return true if the dependent is currently valid
	 */
	public synchronized boolean isValid()
	{
		return true;
	}

	/**
	 * Prepare to mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).
	 *
	 * @param action	The action causing the invalidation
	 * @param p		the provider
	 *
	 * @exception StandardException thrown if unable to make it invalid
	 */
	public void prepareToInvalidate(Provider p, int action,
					LanguageConnectionContext lcc) 
		throws StandardException
	{
		DependencyManager dm = getDataDictionary().getDependencyManager();

		switch (action)
		{
			/*
			** Currently, the only thing we are depenedent
			** on is an alias.
			*/
		    default:
				DataDictionary dd = getDataDictionary();
				ColumnDescriptor cd = dd.getColumnDescriptorByDefaultId(defaultUUID);
				TableDescriptor td = dd.getTableDescriptor(cd.getReferencingUUID());

				throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, 
									dm.getActionString(action), 
									p.getObjectName(),
									MessageService.getTextMessage(
										SQLState.LANG_COLUMN_DEFAULT
									),
									td.getQualifiedName() + "." +
									cd.getColumnName());
		}
	}

	/**
	 * Mark the dependent as invalid (due to at least one of
	 * its dependencies being invalid).  Always an error
	 * for a constraint -- should never have gotten here.
	 *
	 * @param	action	The action causing the invalidation
	 *
	 * @exception StandardException thrown if called in sanity mode
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc) 
		throws StandardException
	{
		/* 
		** We should never get here, we should have barfed on 
		** prepareToInvalidate().
		*/
		if (SanityManager.DEBUG)
		{
			DependencyManager dm;
	
			dm = getDataDictionary().getDependencyManager();

			SanityManager.THROWASSERT("makeInvalid("+
				dm.getActionString(action)+
				") not expected to get called");
		}
	}
}
