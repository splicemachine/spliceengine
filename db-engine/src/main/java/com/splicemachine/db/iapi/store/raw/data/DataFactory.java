/*

   Derby - Class com.splicemachine.db.iapi.store.raw.data.DataFactory

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

package com.splicemachine.db.iapi.store.raw.data;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.raw.RecordHandle;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.db.catalog.UUID;

public interface DataFactory {

	public static final String MODULE = "com.splicemachine.db.iapi.store.raw.data.DataFactory";

	/**
		The temporary segment is called "tmp"
	 */
	public static final String TEMP_SEGMENT_NAME = "tmp";

	/**
		The database lock
	 */
	public static final String DB_LOCKFILE_NAME =  "db.lck";

	/**
	** file name that is used to acquire exclusive lock on DB.
	**/
	public static final String DB_EX_LOCKFILE_NAME = "dbex.lck";

	/**
		Is the store read-only.
	*/
	public boolean isReadOnly();

	public void checkpoint() throws StandardException;

	public void idle() throws StandardException;

	/**
		Return the identifier that uniquely identifies this raw store at runtime.
		This identifier is to be used as part of the lokcing key for objects
		locked in the raw store by value (e.g. Containers).
	*/
	public UUID getIdentifier();

	/**
		Return a record handle that is initialized to the given page number and
        record id.

		@exception StandardException Standard Derby exception policy.

		@param segmentId    segment where the RecordHandle belongs.
		@param containerId  container where the RecordHandle belongs.
		@param pageNumber   the page number of the RecordHandle.
		@param recordId     the record id of the RecordHandle.

		@see RecordHandle
	*/
//	public RecordHandle makeRecordHandle(long segmentId, long containerId, long pageNumber, int recordId)
//		 throws	StandardException;


	/**
		Encrypt cleartext into ciphertext.

		@see com.splicemachine.db.iapi.services.crypto.CipherProvider#encrypt
		@exception StandardException Standard Derby Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length,
					   byte[] ciphertext, int outputOffset, 
                       boolean newEngine)
		 throws StandardException ;

	/**
		Decrypt cleartext from ciphertext.

		@see com.splicemachine.db.iapi.services.crypto.CipherProvider#decrypt
		@exception StandardException Standard Derby Error Policy
	 */
	public int decrypt(byte[] ciphertext, int offset, int length,
					   byte[] cleartext, int outputOffset)
		 throws StandardException ;

	/**
		Return the encryption block size used by the algorithm at time of
		encrypted database creation
	 */
	public int getEncryptionBlockSize();

    /**
     * @return The StorageFactory used by this dataFactory
     */
    public StorageFactory getStorageFactory();

	public void	stop();

    /**
     * Returns if data base is in encrypted mode.
     * @return true if database encrypted false otherwise
     */
    public boolean databaseEncrypted();
}
