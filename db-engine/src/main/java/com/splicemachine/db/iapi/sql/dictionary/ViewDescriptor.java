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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

/**
 * This is the implementation of ViewDescriptor. Users of View descriptors
 * should only use the following methods:
 * <ol>
 * <li> getUUID
 * <li> setUUID
 * <li> getViewText
 * <li> setViewName
 * <li> getCheckOptionType
 * <li> getCompSchemaId
 * </ol>
 *
 * @version 0.1
 */

public final class ViewDescriptor extends TupleDescriptor
	implements UniqueTupleDescriptor, Dependent, Provider
{
	private final int			checkOption;
	private String		viewName;
	private final String		viewText;
	private UUID		uuid;
	private final UUID		compSchemaId;

	public static final	int NO_CHECK_OPTION = 0;

	/**
	 * Constructor for a ViewDescriptor.
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param viewID	The UUID for the view
	 * @param viewName	The name of the view
	 * @param viewText	The text of the query expression from the view definition.
	 * @param checkOption	int check option type
	 * @param compSchemaId	the schemaid to compile in
	 */

	public ViewDescriptor(DataDictionary dataDictionary, UUID viewID, String viewName, String viewText, 
							  int checkOption, UUID compSchemaId)
	{
		super( dataDictionary );

		uuid = viewID;
		this.viewText = viewText;
		this.viewName = viewName;

		/* RESOLVE - No check options for now */
		if (SanityManager.DEBUG)
		{
			if (checkOption != ViewDescriptor.NO_CHECK_OPTION)
			{
				SanityManager.THROWASSERT("checkOption (" + checkOption +
				") expected to be " + ViewDescriptor.NO_CHECK_OPTION);
			}
		}
		this.checkOption = checkOption;
		this.compSchemaId = compSchemaId;
	}

	//
	// ViewDescriptor interface
	//

	/**
	 * Gets the UUID of the view.
	 *
	 * @return	The UUID of the view.
	 */
	public UUID	getUUID()
	{
		return uuid;
	}

	/**
	 * Sets the UUID of the view.
	 *
	 * @param uuid	The UUID of the view.
	 */
	public void	setUUID(UUID uuid)
	{
		this.uuid = uuid;
	}

	/**
	 * Gets the text of the view definition.
	 *
	 * @return	A String containing the text of the CREATE VIEW
	 *		statement that created the view
	 */
	public String	getViewText()
	{
		return viewText;
	}

	/**
	 * Sets the name of the view.
	 *
	 * @param name	The name of the view.
	 */
	public void	setViewName(String name)
	{
		viewName = name;
	}

	/**
	 * Gets an identifier telling what type of check option
	 * is on this view.
	 *
	 * @return	An identifier telling what type of check option
	 *			is on the view.
	 */
	public int	getCheckOptionType()
	{
		return checkOption;
	}

	/**
	 * Get the compilation type schema id when this view
	 * was first bound.
	 *
	 * @return the schema UUID
	 */
	public UUID getCompSchemaId()
	{
		return compSchemaId;
	}


	//
	// Provider interface
	//

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
	    return getDependableFinder(StoredFormatIds.VIEW_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return viewName;
	}

	/**
	 * Get the provider's UUID 
	 *
	 * @return String	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return uuid;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return String		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.VIEW;
	}

	//
	// Dependent Inteface
	//
	/**
		Check that all of the dependent's dependencies are valid.

		@return true if the dependent is currently valid
	 */
	public boolean isValid()
	{
		return true;
	}

	/**
		Prepare to mark the dependent as invalid (due to at least one of
		its dependencies being invalid).

		@param action	The action causing the invalidation
		@param p		the provider

		@exception StandardException thrown if unable to make it invalid
	 */
	public void prepareToInvalidate(Provider p, int action,
					LanguageConnectionContext lcc) 
		throws StandardException
	{
		switch ( action )
		{
			/*
			 * We don't care about creating or dropping indexes or
			 * alter table on an underlying table.
			 */
		    case DependencyManager.CREATE_INDEX:
		    case DependencyManager.DROP_INDEX:
		    case DependencyManager.DROP_COLUMN:
		    case DependencyManager.CREATE_CONSTRAINT:
		    case DependencyManager.ALTER_TABLE:
		    case DependencyManager.CREATE_TRIGGER:
		    case DependencyManager.DROP_TRIGGER:

		    case DependencyManager.BULK_INSERT:
		    case DependencyManager.COMPRESS_TABLE:
		    case DependencyManager.RENAME_INDEX:
			case DependencyManager.UPDATE_STATISTICS:
			case DependencyManager.DROP_STATISTICS:
			case DependencyManager.TRUNCATE_TABLE:
			/*
			** Set constriants is a bit odd in that it
			** will send a SET_CONSTRAINTS on the table
			** when it enables a constraint, rather than
			** on the constraint.  So since we depend on
			** the table, we have to deal with this action.
			*/
		    case DependencyManager.SET_CONSTRAINTS_ENABLE:
		    case DependencyManager.SET_TRIGGERS_ENABLE:
			//When REVOKE_PRIVILEGE gets sent (this happens for privilege 
			//types SELECT, UPDATE, DELETE, INSERT, REFERENCES, TRIGGER), we  
			//don't do anything here. Later in makeInvalid method, we make  
			//the ViewDescriptor drop itself. 
		    case DependencyManager.REVOKE_PRIVILEGE:

			// When a role grant is revoked, any a view dependent on priviliges
			// obtained through that role grant will dropped. We don't do
			// anything here. Later in makeInvalid method, we make the
			// ViewDescriptor drop itself.
			case DependencyManager.REVOKE_ROLE:

			// Only used by Activations
		    case DependencyManager.RECHECK_PRIVILEGES:

				break;
				// When REVOKE_PRIVILEGE gets sent to a
				// TablePermsDescriptor we must also send
				// INTERNAL_RECOMPILE_REQUEST to its Dependents which
				// may be GPSs needing re-compilation. But Dependents
				// could also be ViewDescriptors, which then also need
				// to handle this event.
			case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
		    	break;

			//Notice that REVOKE_PRIVILEGE_RESTRICT is not caught earlier.
		    //It gets handled in this default: action where an exception
		    //will be thrown. This is because, if such an invalidation 
		    //action type is ever received by a dependent, the dependent 
		    //show throw an exception.
			//In Derby, at this point, REVOKE_PRIVILEGE_RESTRICT gets sent
		    //when execute privilege on a routine is getting revoked.
		    // DROP_COLUMN_RESTRICT is similar. Any case which arrives
		    // at this default: statement causes the exception to be
		    // thrown, indicating that the DDL modification should be
		    // rejected because a view is dependent on the underlying
		    // object (table, column, privilege, etc.)
		    default:

				DependencyManager dm;

				dm = getDataDictionary().getDependencyManager();
				throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_VIEW, 
					dm.getActionString(action), 
					p.getObjectName(), viewName);

		}	// end switch
	}

	/**
		Mark the dependent as invalid (due to at least one of
		its dependencies being invalid).

		@param	action	The action causing the invalidation

		@exception StandardException thrown if unable to make it invalid
	 */
	public void makeInvalid(int action, LanguageConnectionContext lcc) 
		throws StandardException
	{
		switch ( action )
		{
			/* We don't care about creating or dropping indexes or
			 * alter table on an underlying table.
			 */
		    case DependencyManager.CREATE_INDEX:
		    case DependencyManager.DROP_INDEX:
		    case DependencyManager.ALTER_TABLE:
		    case DependencyManager.CREATE_CONSTRAINT:
		    case DependencyManager.BULK_INSERT:
		    case DependencyManager.COMPRESS_TABLE:
		    case DependencyManager.SET_CONSTRAINTS_ENABLE:
		    case DependencyManager.SET_TRIGGERS_ENABLE:
		    case DependencyManager.CREATE_TRIGGER:
		    case DependencyManager.DROP_TRIGGER:
		    case DependencyManager.RENAME_INDEX:
			case DependencyManager.UPDATE_STATISTICS:
			case DependencyManager.DROP_STATISTICS:
			case DependencyManager.TRUNCATE_TABLE:

				// Only used by Activations
			case DependencyManager.RECHECK_PRIVILEGES:

				// When REVOKE_PRIVILEGE gets sent to a
				// TablePermsDescriptor we must also send
				// INTERNAL_RECOMPILE_REQUEST to its Dependents which
				// may be GPSs needing re-compilation. But Dependents
				// could also be ViewDescriptors, which then also need
				// to handle this event.
		    case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
				break;

				// When REVOKE_PRIVILEGE gets sent (this happens for privilege
				// types SELECT, UPDATE, DELETE, INSERT, REFERENCES, TRIGGER),
				// we make the ViewDescriptor drop itself. REVOKE_ROLE also
				// drops the dependent view.
            case DependencyManager.DROP_COLUMN:
		    case DependencyManager.REVOKE_PRIVILEGE:
			case DependencyManager.REVOKE_ROLE:
                
                TableDescriptor td = 
                        getDataDictionary().getTableDescriptor(uuid);
                
                if (td == null) { 
                    // DERBY-5567 already dropped via another dependency 
                    break;
                }
                
                // DERBY-5567 keep original action
                drop(lcc, td.getSchemaDescriptor(), td, action);

                lcc.getLastActivation().addWarning(
                        StandardException.newWarning(
                        SQLState.LANG_VIEW_DROPPED,
                        this.getObjectName() ));
                break;

            default:

				/* We should never get here, since we can't have dangling references */
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("did not expect to get called");
				}
				break;

		}	// end switch

	}

	//
	// class interface
	//

	/**
	 * Prints the contents of the ViewDescriptor
	 *
	 * @return The contents as a String
	 */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return	"uuid: " + uuid + " viewName: " + viewName + "\n" +
				"viewText: " + viewText + "\n" +
				"checkOption: " + checkOption + "\n" +
				"compSchemaId: " + compSchemaId + "\n";
		}
		else
		{
			return "";
		}
	}

    /**
     * Drop this descriptor, if not already done.
     * 
     * @param lcc current language connection context
     * @param sd schema descriptor
     * @param td table descriptor for this view
     * @throws StandardException standard error policy
     */
    public void drop(
            LanguageConnectionContext lcc,
            SchemaDescriptor sd,
            TableDescriptor td) throws StandardException
    {
        drop(lcc, sd, td, DependencyManager.DROP_VIEW);
    }

    /**
     * Drop this descriptor, if not already done, due to action.
     * If action is not {@code DependencyManager.DROP_VIEW}, the descriptor is 
     * dropped due to dropping some other object, e.g. a table column.
     * 
     * @param lcc current language connection context
     * @param sd schema descriptor
     * @param td table descriptor for this view
     * @param action action
     * @throws StandardException standard error policy
     */
    private void drop(
            LanguageConnectionContext lcc,
            SchemaDescriptor sd,
            TableDescriptor td,
            int action) throws StandardException
    {
        DataDictionary dd = getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();

		/* Drop the columns */
		dd.dropAllColumnDescriptors(td.getUUID(), tc);

		/* Prepare all dependents to invalidate.  (This is there chance
		 * to say that they can't be invalidated.  For example, an open
		 * cursor referencing a table/view that the user is attempting to
		 * drop.) If no one objects, then invalidate any dependent objects.
		 */
        dm.invalidateFor(td, action, lcc);

		/* Clear the dependencies for the view */
		dm.clearDependencies(lcc, this);

		/* Drop the view */
		dd.dropViewDescriptor(this, tc);

		/* Drop all table and column permission descriptors */
		dd.dropAllTableAndColPermDescriptors(td.getUUID(), tc);

		/* Drop the table */
		dd.dropTableDescriptor(td, sd, tc);
	}

    public String getName() {
        return viewName;
    }

}
