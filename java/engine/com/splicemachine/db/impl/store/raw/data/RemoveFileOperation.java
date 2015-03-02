/*

   Derby - Class com.splicemachine.db.impl.store.raw.data.RemoveFileOperation

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

import com.splicemachine.db.iapi.store.raw.Loggable;
import com.splicemachine.db.iapi.store.raw.Undoable;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.store.raw.Compensation;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.store.raw.xact.RawTransaction;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.iapi.store.raw.log.LogInstant;

import com.splicemachine.db.io.StorageFile;

import com.splicemachine.db.iapi.util.ByteArray;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

import com.splicemachine.db.iapi.services.io.LimitObjectInput;

/** 
*/

public class RemoveFileOperation implements Undoable
{
	private String name;
	private long generationId;
	private boolean removeAtOnce;

	transient private StorageFile fileToGo;

	// no-arg constructor, required by Formatable
	public RemoveFileOperation()
	{
	}

	RemoveFileOperation(String name, long generationId, boolean removeAtOnce)
	{
		this.name = name;
		this.generationId = generationId;
		this.removeAtOnce = removeAtOnce;
	}

	/*
	 * Formatable methods
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeUTF(name);
		out.writeLong(generationId);
		out.writeBoolean(removeAtOnce);
	}

	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException 
	{
		name = in.readUTF();
		generationId = in.readLong();
		removeAtOnce = in.readBoolean();
	}
	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_REMOVE_FILE;
	}


	/**
		Loggable methods
	*/

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		Space Operation has no optional data to write out
	*/
	public ByteArray getPreparedLog()
	{
		return null;
	}

	public void releaseResource(Transaction tran)
	{
	}

	/**
		A space operation is a RAWSTORE log record
	*/
	public int group()
	{
		return Loggable.FILE_RESOURCE | Loggable.RAWSTORE ;
	}

	public void doMe(Transaction xact, LogInstant instant, 
						   LimitObjectInput in)
		 throws StandardException
	{
		if (fileToGo == null)
			return;

		BaseDataFileFactory bdff = 
			(BaseDataFileFactory) ((RawTransaction) xact).getDataFactory();
		
		bdff.fileToRemove(fileToGo, true);
	}


	/**
		@exception StandardException Standard Derby error policy
	*/
	public boolean needsRedo(Transaction xact)
		 throws StandardException
	{
		if (!removeAtOnce)
			return false;

		FileResource fr = ((RawTransaction) xact).getDataFactory().getFileHandler();

		fileToGo = fr.getAsFile(name, generationId);

		if (fileToGo == null)
			return false;

        return fileToGo.exists();
	}


	public Compensation generateUndo(Transaction xact, LimitObjectInput in)
		throws StandardException, IOException {


		if (fileToGo != null) {
			BaseDataFileFactory bdff = 
				(BaseDataFileFactory) ((RawTransaction) xact).getDataFactory();
		
			bdff.fileToRemove(fileToGo, false);
		}

		return null;
	}
}


