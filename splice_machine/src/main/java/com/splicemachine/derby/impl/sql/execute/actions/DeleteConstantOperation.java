/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.impl.sql.execute.FKInfo;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	This class  describes compiled constants that are passed into
 *	DeleteResultSets.
 *
 */
public class DeleteConstantOperation extends WriteCursorConstantOperation {
	private static final Logger LOG = Logger.getLogger(DeleteConstantOperation.class);	
	int numColumns;
	ConstantAction[] dependentCActions; //constant action for the dependent table
	ResultDescription resultDescription; //required for dependent tables.

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	DeleteConstantOperation() { 
    	super(); 
    	SpliceLogUtils.trace(LOG, "DeleteConstantOperation");
    }

	/**
	 *	Make the ConstantAction for an DELETE statement.
	 *
	 *  @param conglomId	Conglomerate ID.
	 *	@param heapSCOCI	StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs			Index descriptors
	 *  @param indexCIDS	Conglomerate IDs of indices
	 *	@param indexSCOCIs	StaticCompiledOpenConglomInfos for indexes.
	 *  @param emptyHeapRow	Template for heap row.
	 *  @param deferred		True means process as a deferred insert.
	 *	@param targetUUID	UUID of target table
	 *	@param lockMode		The lock mode to use
	 *							(row or table, see TransactionController)
	 *	@param fkInfo		Array of structures containing foreign key info, if any (may be null)
	 *	@param triggerInfo	Array of structures containing trigger info, if any (may be null)
	 *  @param baseRowReadList Map of columns read in.  1 based.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param numColumns	Number of columns to read.
	 *  @param singleRowSource		Whether or not source is a single row source
	 */
	@SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
	public	DeleteConstantOperation(
								long				conglomId,
								StaticCompiledOpenConglomInfo heapSCOCI,
                                int[] pkColumns,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								ExecRow				emptyHeapRow,
								boolean				deferred,
								UUID				targetUUID,
								int					lockMode,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								FormatableBitSet				baseRowReadList,
								int[]				baseRowReadMap,
								int[]               streamStorableHeapColIds,
								int					numColumns,
								boolean				singleRowSource,
								ResultDescription   resultDescription,
								ConstantAction[] dependentCActions) {
		super( conglomId, 
			   heapSCOCI,
                pkColumns,
			   irgs, indexCIDS, indexSCOCIs,
			   null, // index names not needed for delete.
			   deferred,
				null,
			   targetUUID,
			   lockMode,
			   fkInfo,
			   triggerInfo,
			   emptyHeapRow,
			   baseRowReadList,
			   baseRowReadMap,
			   streamStorableHeapColIds,
			   singleRowSource
			   );

    	SpliceLogUtils.trace(LOG, "DeleteConstantOperation instance");
		
		this.numColumns = numColumns;
		this.resultDescription = resultDescription;
		this.dependentCActions =  dependentCActions;
	}

	/**
	  @see java.io.Externalizable#readExternal
	  @exception IOException thrown on error
	  @exception ClassNotFoundException	thrown on error
	  */
	public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		numColumns = in.readInt();
		//information required for cascade delete
		dependentCActions = new ConstantAction[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, dependentCActions);
		resultDescription = (ResultDescription) in.readObject();
	}

	/**

	  @exception IOException thrown on error
	  */
	public void writeExternal( ObjectOutput out ) throws IOException {
		super.writeExternal(out);
		out.writeInt(numColumns);

		//write cascade delete information
		ArrayUtil.writeArray(out, dependentCActions);
		out.writeObject(resultDescription);

	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ 
		return StoredFormatIds.DELETE_CONSTANT_ACTION_V01_ID; 
	}

}
