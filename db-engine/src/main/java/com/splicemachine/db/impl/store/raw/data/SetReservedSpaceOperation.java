/*

   Derby - Class com.splicemachine.db.impl.store.raw.data.SetReservedSpaceOperation

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

import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.store.raw.Page;
import com.splicemachine.db.iapi.store.raw.Transaction;

import com.splicemachine.db.iapi.store.raw.log.LogInstant;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.io.CompressedNumber;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import com.splicemachine.db.iapi.services.io.LimitObjectInput;

/**
	Represents shrinking of the reserved space of a particular row on a page.
	This operation is not undoable.
*/
public class SetReservedSpaceOperation extends PageBasicOperation {

	protected int	doMeSlot;	// slot where record is at
	protected int	recordId;	// recordId
	protected int	newValue;	// the new reserved space value
	protected int	oldValue;	// the old reserved space value (for BI_logging)

	public SetReservedSpaceOperation(BasePage page, int slot, 
									 int recordId, int newValue, int oldValue)
	{
		super(page);
		doMeSlot = slot;
		this.recordId = recordId;
		this.newValue = newValue;
		this.oldValue = oldValue;

		if (SanityManager.DEBUG) // we only use this for shrinking
			SanityManager.ASSERT(oldValue > newValue);
	}

	/*
	 * Formatable methods
	 */
	// no-arg constructor, required by Formatable 
	public SetReservedSpaceOperation() { super(); }

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_SET_RESERVED_SPACE;
	}

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);

		CompressedNumber.writeInt(out, doMeSlot);
		CompressedNumber.writeInt(out, recordId);
		CompressedNumber.writeInt(out, newValue);
		CompressedNumber.writeInt(out, oldValue);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
		@exception ClassNotFoundException log stream corrupted
	*/
	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		doMeSlot = CompressedNumber.readInt(in);
		recordId = CompressedNumber.readInt(in);
		newValue = CompressedNumber.readInt(in);
		oldValue = CompressedNumber.readInt(in);
	}

	/*
	 * Loggable methods
	 */
	/**
		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Derby policy.		
	  
		@see com.splicemachine.db.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(oldValue ==
								 this.page.getReservedCount(doMeSlot));
			SanityManager.ASSERT(newValue < oldValue,
				"cannot set reserved space to be bigger than before"); 
		}

		page.setReservedSpace(instant, doMeSlot, newValue);
	}

	/*
	 * method to support BeforeImageLogging - This log operation is not
	 * undoable in the logical sense , but all log operations that touch a page
	 * must support physical undo during RRR transaction.
	 */

	/**
	 * restore the before image of the page
	 *
	 * @exception StandardException Standard Derby Error Policy
	 * @exception IOException problem reading the complete log record from the
	 * input stream
	 */

	public void restoreMe(Transaction xact, BasePage undoPage, LogInstant CLRinstant, LimitObjectInput in)
		 throws StandardException, IOException
	{
		int slot = undoPage.findRecordById(recordId,Page.FIRST_SLOT_NUMBER);
		if (SanityManager.DEBUG)
		{
			if ( ! getPageId().equals(undoPage.getPageId()))
				SanityManager.THROWASSERT(
								"restoreMe cannot restore to a different page. "
								 + "doMe page:" + getPageId() + " undoPage:" + 
								 undoPage.getPageId());
			if (slot != doMeSlot)
				SanityManager.THROWASSERT(
								"restoreMe cannot restore to a different slot. "
								 + "doMe slot:" + doMeSlot + " undoMe slot: " +
								 slot + " recordId:" + recordId);

		}

		page.setReservedSpace(CLRinstant, slot, oldValue);

	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
				"Set Reserved space of recordId " + recordId + " from " + oldValue + " to " + newValue;
		}
		return null;
	}
}
