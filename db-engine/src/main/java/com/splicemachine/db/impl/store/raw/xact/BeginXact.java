/*

   Derby - Class com.splicemachine.db.impl.store.raw.xact.BeginXact

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

package com.splicemachine.db.impl.store.raw.xact;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.store.raw.Loggable;
import com.splicemachine.db.iapi.store.raw.GlobalTransactionId;

import com.splicemachine.db.iapi.store.raw.log.LogInstant;
import com.splicemachine.db.iapi.store.raw.xact.RawTransaction;

import com.splicemachine.db.iapi.util.ByteArray;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import com.splicemachine.db.iapi.services.io.LimitObjectInput;

/**
	This operation indicates the beginning of a transaction.
	@see Loggable
*/

public class BeginXact implements Loggable {

	protected int transactionStatus;
	protected GlobalTransactionId xactId;


	public BeginXact(GlobalTransactionId xid, int s)
	{
		xactId = xid;
		transactionStatus = s;
	}

	/*
	 * Formatable methods
	 */
	public BeginXact()
	{  super() ; }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		out.writeInt(transactionStatus);
		out.writeObject(xactId);
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		transactionStatus = in.readInt();
		xactId = (GlobalTransactionId)in.readObject();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_BEGIN_XACT;
	}

	/**
		Loggable methods
		@see Loggable
	*/

	/**
		Apply the change indicated by this operation and optional data.

		@param xact			the Transaction
		@param instant		the log instant of this operation
		@param in			optional data

	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
	{
		RawTransaction rt = (RawTransaction)xact;

		// If we are not doing fake logging for in memory database
		if (instant != null) 
		{
			rt.setFirstLogInstant(instant);

			// need to do this here rather than in the transaction object for
			// recovery.
			rt.addUpdateTransaction(transactionStatus);
		}
	}

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		BeginXact has no optional data to write out

		@see ObjectOutput
	*/
	public ByteArray getPreparedLog()
	{
		return (ByteArray) null;
	}

	/**
		Always redo a BeginXact.

		@param xact		The transaction trying to redo this operation
		@return true if operation needs redoing, false if not.
	*/
	public boolean needsRedo(Transaction xact)
	{
		return true;			// always redo this
	}


	/**
		BeginXact has no resource to release
	*/
	public void releaseResource(Transaction xact)
	{}


	/**
		BeginXact is both a FIRST and a RAWSTORE log record
	*/
	public int group()
	{
		int group = Loggable.FIRST | Loggable.RAWSTORE;
		return group;
	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
			return "BeginXact " + xactId + " transactionStatus " + Integer.toHexString(transactionStatus);
		else
			return null;

	}

	/**
		BeginXact method
	*/
	public GlobalTransactionId getGlobalId()
	{
		return xactId;
	}


}

