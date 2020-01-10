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

/**
	A CipherProvider is a wrapper for a Cipher class in JCE.

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

public interface CipherProvider
{

	/**
		Encrypt data - use only with Cipher that has been initialized with
		CipherFactory.ENCRYPT.

		@return The number of bytes stored in ciphertext.

		@param cleartext the byte array containing the cleartext
		@param offset encrypt from this byte offset in the cleartext
		@param length encrypt this many bytes starting from offset
		@param ciphertext the byte array to store the ciphertext
		@param outputOffset the offset into the ciphertext array the output
				should go

		If cleartext and ciphertext are the same array, caller must be careful
		to not overwrite the cleartext before it is scrambled.

		@exception StandardException Standard Derby Error Policy
	 */
	int encrypt(byte[] cleartext, int offset, int length,
				byte[] ciphertext, int outputOffset)
		 throws StandardException;

	/**
		Decrypt data - use only with Cipher that has been initialized with
		CipherFactory.DECRYPT.

		@return The number of bytes stored in cleartext.

		@param ciphertext the byte array containing the ciphertext
		@param offset decrypt from this byte offset in the ciphertext
		@param length decrypt this many bytes starting from offset
		@param cleartext the byte array to store the cleartext
		@param outputOffset the offset into the cleartext array the output
				should go

		If cleartext and ciphertext are the same array, caller must be careful
		to not overwrite the ciphertext before it is un-scrambled.

		@exception StandardException Standard Derby Error Policy
	 */
	int decrypt(byte[] ciphertext, int offset, int length,
				byte[] cleartext, int outputOffset)
		 throws StandardException;


	/**
	 	Returns the encryption block size used during creation of the encrypted database
	 */
	int getEncryptionBlockSize();
}
