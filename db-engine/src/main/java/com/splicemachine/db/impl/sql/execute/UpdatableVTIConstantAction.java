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


import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.sql.execute.ExecRow;

/**
 *	This class  describes compiled constants that are passed into
 *	Updatable VTIResultSets.
 *
 */

public class UpdatableVTIConstantAction extends WriteCursorConstantAction
{

	/********************************************************
	**
	**	This class implements Formatable. But it is NOT used
 	**	across either major or minor releases.  It is only
	** 	written persistently in stored prepared statements, 
	**	not in the replication stage.  SO, IT IS OK TO CHANGE
	**	ITS read/writeExternal.
	**
	********************************************************/

	public int[]	changedColumnIds;

    public int statementType;
    
	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	UpdatableVTIConstantAction() { super(); }

	/**
	 *	Make the ConstantAction for an updatable VTI statement.
	 *
	 * @param deferred					Whether or not to do operation in deferred mode
     * @param changedColumnIds Array of ids of changed columns
	 *
	 */
	public	UpdatableVTIConstantAction( int statementType,
                                        boolean deferred,
                                        int[] changedColumnIds)
	{
		super(0, 
			  null,
			  null, 
			  null, 
			  null,
			  null,
                null,
			  deferred	, 
			  null,
			  null,
			  0,
			  null,	
			  null,
			  (ExecRow)null, // never need to pass in a heap row
			  null,
			  null,
			  null,
			  // singleRowSource, irrelevant
			  false
			  );
        this.statementType = statementType;
        this.changedColumnIds = changedColumnIds;
	}

	// INTERFACE METHODS

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.UPDATABLE_VTI_CONSTANT_ACTION_V01_ID; }

	// CLASS METHODS

}
