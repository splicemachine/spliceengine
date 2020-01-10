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

package com.splicemachine.db.iapi.services.crypto;

import com.splicemachine.db.iapi.error.StandardException;
import java.security.SecureRandom;
import java.util.Properties;
import com.splicemachine.db.io.StorageFactory;

/**
	A CipherFactory can create new CipherProvider, which is a wrapper for a
	javax.crypto.Cipher

	This service is only available when run on JDK1.2 or beyond.
	To use this service, either the SunJCE or an alternative clean room
    implementation of the JCE must be installed.

	To use a CipherProvider to encrypt or decrypt, it needs 3 things:
	1) A CipherProvider that is initialized to ENCRYPT or DECRYPT
	2) A secret Key for the encryption/decryption
	3) An Initialization Vector (IvParameterSpec) that is used to create some
		randomness in the encryption

    See $WS/docs/funcspec/mulan/configurableEncryption.html

	See http://java.sun.com/products/JDK/1.1/docs/guide/security/CryptoSpec.html
	See http://java.sun.com/products/JDK/1.2/docs/guide/security/CryptoSpec.html
	See http://java.sun.com/products/jdk/1.2/jce/index.html
 */

public interface CipherFactory
{

    /** Minimum bootPassword length */
	int MIN_BOOTPASS_LENGTH = 8;

	/**
		Get a CipherProvider that either Encrypts or Decrypts.
	 */
	int ENCRYPT = 1;
	int DECRYPT = 2;


	SecureRandom getSecureRandom();

	/**
		Returns a CipherProvider which is the encryption or decryption engine.
		@param mode is either ENCRYPT or DECRYPT.  The CipherProvider can only
				do encryption or decryption but not both.

		@exception StandardException Standard Derby Error Policy
	 */
	CipherProvider createNewCipher(int mode)
		 throws StandardException;

	String changeBootPassword(String changeString, Properties properties, CipherProvider verify)
		throws StandardException;

	/**
		Verify the external encryption key. Throws exception if unable to verify
		that the encryption key is the same as that
		used during database creation or if there are any problems when trying to do the
		verification process.
		
		@param	create	 true means database is being created, whereas false
					implies that the database has already been created
		@param	storageFactory storageFactory is used to access any stored data
					that might be needed for verification process of the encryption key
		@param	properties	properties at time of database connection as well as those in service.properties
	 */
	void verifyKey(boolean create, StorageFactory storageFactory, Properties properties)
		throws StandardException;

    void saveProperties(Properties properties);

}


