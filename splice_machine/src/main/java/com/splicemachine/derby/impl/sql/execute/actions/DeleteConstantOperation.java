/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
	 *  @param conglomId	Record ID.
	 *	@param heapSCOCI	StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs			Index descriptors
	 *  @param indexCIDS	Record IDs of indices
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
