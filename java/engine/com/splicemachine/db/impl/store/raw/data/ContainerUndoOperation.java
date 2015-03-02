/*

   Derby - Class com.splicemachine.db.impl.store.raw.data.ContainerUndoOperation

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

package com.splicemachine.db.impl.store.raw.data;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.store.raw.Compensation;
import com.splicemachine.db.iapi.store.raw.Loggable;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.store.raw.Undoable;

import com.splicemachine.db.iapi.store.raw.data.RawContainerHandle;
import com.splicemachine.db.iapi.store.raw.log.LogInstant;

import com.splicemachine.db.iapi.error.StandardException;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import com.splicemachine.db.iapi.services.io.LimitObjectInput;

/** A Container undo operation rolls back the change of a Container operation */
public class ContainerUndoOperation extends ContainerBasicOperation 
		implements Compensation 
{
	// the operation to rollback 
	transient private	ContainerOperation undoOp;

	/** During redo, the whole operation will be reconstituted from the log */

	/** 
		Set up a Container undo operation during run time rollback
		@exception StandardException container Handle is not active
	*/
	public ContainerUndoOperation(RawContainerHandle hdl, ContainerOperation op) 
		 throws StandardException
	{
		super(hdl);
		undoOp = op;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public ContainerUndoOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
	}

	/**
		@exception IOException cannot read log record from log stream
		@exception ClassNotFoundException cannot read ByteArray object
	 */
	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_CONTAINER_UNDO;
	}

	/** 
		Compensation method
	*/

	/** Set up a Container undo operation during recovery redo. */
	public void setUndoOp(Undoable op)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(op instanceof ContainerOperation);
		}

		undoOp = (ContainerOperation)op;
	}

	/**
		Loggable methods
	*/

	/** Apply the undo operation, in this implementation of the
		RawStore, it can only call the undoMe method of undoOp

		@param xact			the Transaction that is doing the rollback
		@param instant		the log instant of this compenstaion operation
		@param in			optional data

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Derby error policy.

		@see ContainerOperation#generateUndo
	 */
	public final void doMe(Transaction xact, LogInstant instant, LimitObjectInput in) 
		 throws StandardException, IOException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(containerHdl != null, "clr has null containerHdl");
		}

		// if this is called during runtime rollback, generateUndo found
		// the container and have it opened there.
		// if this is called during recovery redo, this.needsRedo found 
		// the container and have it opened here.
		//
		// in either case, containerHdl is the opened container handle.

		undoOp.undoMe(xact, containerHdl, instant, in);
		releaseResource(xact);
	}

	/* make sure resource found in undoOp is released */
	public void releaseResource(Transaction xact)
	{
		if (undoOp != null)
			undoOp.releaseResource(xact);
		super.releaseResource(xact);
	}

	/* Undo operation is a COMPENSATION log operation */
	public int group()
	{
		return super.group() | Loggable.COMPENSATION | Loggable.RAWSTORE;
	}

}
