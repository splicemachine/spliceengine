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

package com.splicemachine.db.impl.services.jce;

import com.splicemachine.db.iapi.services.crypto.CipherFactory;
import com.splicemachine.db.iapi.services.crypto.CipherProvider;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.GeneralSecurityException;
import java.security.NoSuchProviderException;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.SecretKey;


/**
	This is a wrapper for a Cipher

	@see CipherFactory
 */
class JCECipherProvider implements CipherProvider
{
	private Cipher cipher;
	private int mode;
    private boolean ivUsed = true;
    private final IvParameterSpec ivspec;
    private final int encryptionBlockSize;
    private boolean sunjce; //default of bool is false

    // other provider workaround, we need to re-init the cipher before every encrypt/decrypt
    private SecretKey cryptixKey;

	JCECipherProvider(int mode, SecretKey secretKey, byte[] iv, String algorithm, String provider)
		 throws StandardException
	{
		Throwable t;
		ivspec = new IvParameterSpec(iv);
		try
		{


			if (provider == null)
			{
				cipher = Cipher.getInstance(algorithm);

				// see below.
				if ("SunJCE".equals(cipher.getProvider().getName()))
					sunjce = true;
			}
			else
			{
				/* The Sun encryption provider does not need to re-init the cipher
				 * after each encrypt/decrypt.  This is a speed up trick.
				 * Other crypto providers needs this because the encrypt/decrypt
				 * ciphers becomes out of sync after an encrypt/decrypt operation.
				 */
				if( provider.equals("SunJCE") )
				{
					sunjce = true;
				}
				else
				{
					/* The BouncyCastle encryption provider is named "BC".
					 * The full "BouncyCastleProvider" name used to work until
					 * version 103 came out.  (ie. Beta3 and Beta4 works fine)
					 * This trick is so that Cipher.getInstance(algo, prov) will
					 * not throw an exception.  Resolve 3765.
					 */
					if( provider.equals( "BouncyCastleProvider" ) )
						provider = "BC";
				}

				cipher = Cipher.getInstance(algorithm,provider);
			}

			// At creation time, the encryption block size is stored in order
			// to do appropriate padding
			encryptionBlockSize = cipher.getBlockSize();

			this.mode = mode;
			try {

				// ECB feedback mode does not require an IV
				if (mode == CipherFactory.ENCRYPT)
				{
					if ((algorithm.contains("/ECB")))
						cipher.init(Cipher.ENCRYPT_MODE, secretKey);
					else
						cipher.init(Cipher.ENCRYPT_MODE, secretKey,ivspec);
				}
				else if (mode == CipherFactory.DECRYPT)
				{
					if ((algorithm.contains("/ECB")))
						cipher.init(Cipher.DECRYPT_MODE, secretKey);
					else
						cipher.init(Cipher.DECRYPT_MODE, secretKey,ivspec);
				}
				else
					throw StandardException.newException(SQLState.ILLEGAL_CIPHER_MODE);
			} catch (InvalidKeyException ike) {

				if (algorithm.startsWith("DES")) {

					SecretKeyFactory skf;
					if (provider == null)
						skf = SecretKeyFactory.getInstance(secretKey.getAlgorithm());
					else
						skf = SecretKeyFactory.getInstance(secretKey.getAlgorithm(), provider);


					// Since the key may be a series of bytes generated by an arbitary means
					// we need to translate it into a key suitable for the algorithm.
					secretKey = skf.translateKey(new SecretKeySpec(secretKey.getEncoded(), secretKey.getAlgorithm()));

					// ECB mode does not require IV
					if (mode == CipherFactory.ENCRYPT )
					{
						if ((algorithm.contains("/ECB")))
							cipher.init(Cipher.ENCRYPT_MODE, secretKey);
						else
							cipher.init(Cipher.ENCRYPT_MODE, secretKey,ivspec);
					}
					else if (mode == CipherFactory.DECRYPT)
					{
						if ((algorithm.contains("/ECB")))
							cipher.init(Cipher.DECRYPT_MODE, secretKey);
						else
							cipher.init(Cipher.DECRYPT_MODE, secretKey,ivspec);
					}

				}
				else
					throw StandardException.newException(SQLState.CRYPTO_EXCEPTION, ike);
			}
            cryptixKey = secretKey;

            if (cipher.getIV() == null)
                ivUsed = false;

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(verifyIV(iv));

			return;

		}
		catch (InvalidKeyException ike)
		{
			t = ike;
		}
		catch (NoSuchAlgorithmException nsae)
		{
    		throw StandardException.newException(SQLState.ENCRYPTION_NOSUCH_ALGORITHM, algorithm, JCECipherFactory.providerErrorName(provider));
		}
        catch (NoSuchProviderException nspe)
        {
    		throw StandardException.newException(SQLState.ENCRYPTION_BAD_PROVIDER, JCECipherFactory.providerErrorName(provider));
        }
		catch (GeneralSecurityException gse)
		{
			t = gse;
		}
		throw StandardException.newException(SQLState.CRYPTO_EXCEPTION, t);

	}

	/**
		@see CipherProvider#encrypt

		@exception StandardException Standard Derby Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length,
					   byte[] ciphertext, int outputOffset)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(mode == CipherFactory.ENCRYPT,
								 "calling encrypt on a decryption engine");
			SanityManager.ASSERT(cleartext != null, "encrypting null cleartext");
			SanityManager.ASSERT(offset >= 0, "offset < 0");
			SanityManager.ASSERT(length > 0, "length <= 0");
			SanityManager.ASSERT(offset+length <= cleartext.length,
								 "offset+length > cleartext.length");
			SanityManager.ASSERT(length <= ciphertext.length-outputOffset,
								 "provided ciphertext buffer insufficient");
		}

		int retval = 0;
		try
		{
			// this same cipher is shared across the entire raw store, make it
			// MT safe
			synchronized(this)
			{
                if( !sunjce )
                {
                    // this code is a workaround for other providers
                    try
                    {
			            //ivspec = new IvParameterSpec(cipher.getIV());
    			        if (mode == CipherFactory.ENCRYPT)
    			        {
							if (ivUsed)
	    			        	cipher.init(Cipher.ENCRYPT_MODE, cryptixKey, ivspec);
	    			        else
	    			    		cipher.init(Cipher.ENCRYPT_MODE,cryptixKey);
						}
	    			    else if (mode == CipherFactory.DECRYPT)
	    			    {
							if (ivUsed)
			    	        	cipher.init(Cipher.DECRYPT_MODE, cryptixKey, ivspec);
			    	    	else
								cipher.init(Cipher.DECRYPT_MODE, cryptixKey);
						}

                    }
            		catch (InvalidKeyException ike)
		            {
						System.out.println("A " + ike);
			            throw StandardException.newException(SQLState.CRYPTO_EXCEPTION, ike);
		            }
                }

				retval = cipher.doFinal(cleartext, offset, length, ciphertext, outputOffset);
			}
		}
		catch (IllegalStateException ise)
		{
			// should never happen
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT(ise);
		}
		catch (GeneralSecurityException gse)
		{
						System.out.println("B " + gse);
			throw StandardException.newException(SQLState.CRYPTO_EXCEPTION, gse);
		}

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(retval == length, "ciphertext length != length");

		return retval;
	}


	/**
		@see CipherProvider#decrypt

		@exception StandardException Standard Derby Error Policy
	 */
	public int decrypt(byte[] ciphertext, int offset, int length,
					   byte[] cleartext, int outputOffset)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(mode == CipherFactory.DECRYPT,
								 "calling decrypt on a encryption engine");
			SanityManager.ASSERT(ciphertext != null, "decrypting null ciphertext");
			SanityManager.ASSERT(offset >= 0, "offset < 0");
			SanityManager.ASSERT(length > 0, "length <= 0");
			SanityManager.ASSERT(offset+length <= ciphertext.length,
								 "offset+length > ciphertext.length");
			SanityManager.ASSERT(length <= cleartext.length-outputOffset,
								 "provided cleartexte buffer insufficient");
		}

		int retval = 0;
		try
		{
			// this same cipher is shared across the entire raw store, make it
			// MT safe
			synchronized(this)
			{
                if( !sunjce )
                {
                    // this code is a workaround for other providers
                    try
                    {
			            //ivspec = new IvParameterSpec(cipher.getIV());

			            if (mode == CipherFactory.ENCRYPT)
						{
							if (ivUsed)
								cipher.init(Cipher.ENCRYPT_MODE, cryptixKey, ivspec);
							else
								cipher.init(Cipher.ENCRYPT_MODE,cryptixKey);
						}
						else if (mode == CipherFactory.DECRYPT)
						{
							if (ivUsed)
								cipher.init(Cipher.DECRYPT_MODE, cryptixKey, ivspec);
							else
								cipher.init(Cipher.DECRYPT_MODE, cryptixKey);
						}

                    }
            		catch (InvalidKeyException ike)
		            {
						System.out.println("C " + ike);
			            throw StandardException.newException(SQLState.CRYPTO_EXCEPTION, ike);
		            }

                }

				retval = cipher.doFinal(ciphertext, offset, length, cleartext, outputOffset);
			}
		}
		catch (IllegalStateException ise)
		{
			// should never happen
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT(ise);
		}
		catch (GeneralSecurityException gse)
		{
						System.out.println("D " + gse);
			throw StandardException.newException(SQLState.CRYPTO_EXCEPTION, gse);
		}

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(retval == length,
								 "cleartext length != length");
		}

		return retval;
	}

	boolean verifyIV(byte[] IV)
	{
		byte[] myIV = cipher.getIV();
        // null IV is OK only if IV is not used
        if (myIV == null)
            return !ivUsed;
		if (myIV.length != IV.length)
			return false;
		for (int i = 0; i < IV.length; i++)
			if (myIV[i] != IV[i])
				return false;
		return true;
	}

	public int getEncryptionBlockSize()
	{
		return encryptionBlockSize;
	}
}
