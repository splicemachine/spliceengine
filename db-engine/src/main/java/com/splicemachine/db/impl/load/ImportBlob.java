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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.load;
import com.splicemachine.db.iapi.services.io.LimitInputStream;
import com.splicemachine.db.impl.jdbc.Util;

import java.sql.Blob;
import java.sql.SQLException;
import java.io.InputStream;

/**
 * This class implements  <code > java.sql.BLOB interface </code>.  
 * Objects created using the <code> ImportBlob </code> class  are 
 * intended to be be used to create a blob object of the data  stored 
 * in an import file or as an hex string.  Only the routines that 
 * are needed read the blob data for the blob columns by the 
 * inserts done through the VTI  have real implementations, 
 * Other routines are dummy ones to satisfy <code> java.sql.Blob </code> 
 * interface.
 */

class ImportBlob implements java.sql.Blob {

    private ImportLobFile lobFile;
	private long blobPosition;
	private long blobLength;
    private byte[] blobData = null;


    /**
     * Create a import Blob object, that reads <code> length </code> amount of 
     * data  from an external file, starting at <code> position </code>. 
     * @param lobFile  lob file resource object, using which data is read.
     * @param position  byte offset in the file, of this blob columb data. 
     * @param length   length of this blob object data. 
     */
    public ImportBlob(ImportLobFile lobFile, long position, long length) 
	{
		this.lobFile = lobFile;
		this.blobPosition = position;
		this.blobLength = length;
	}


    /**
     * Create a import Blob object, whose value is the give hex data string.  
     * @param data  byte array that contains the blob data. 
     */
    public ImportBlob(byte[] data) 
    {
        blobData = data;
        blobLength = data.length;
    }


  /**
   * Returns the number of bytes in this <code>BLOB</code>  object.
   * @return length of the <code>BLOB</code> in bytes
   * @exception SQLException on any error.
   */
	public long length() throws SQLException {
		return blobLength;
	}


    /**
     * Returns  <code>BLOB</code> value designated by this
     * <code>Blob</code> object as a input stream.
     *
     * @return a stream containing the <code>BLOB</code> data
     * @exception SQLException if any error occurs while setting up 
     *                         this blob data in the import file as stream. 
     */
	public java.io.InputStream getBinaryStream () throws SQLException
	{
		try {
            InputStream fis;
            if(blobData != null) {
                fis = new java.io.ByteArrayInputStream(blobData);
                // wrap the InputStream with a LimitInputStream class,
                // only the length of the  
                LimitInputStream  limitIn = new  LimitInputStream(fis);
                limitIn.setLimit((int) blobLength);
                return limitIn;

            } else {
                return lobFile.getBinaryStream(blobPosition, blobLength);
            }
		} catch (Exception e) {
			throw LoadError.unexpectedError(e);
		}
	}

    
    /** following rotines does not have implmentation because there are not
     * used by the VTI that is used to import the data. 
     */

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementatio  is provided , an exception is thrown if used.  
     *
     * @see java.sql.Blob 
     */
	public byte[] getBytes(long pos, int length) throws SQLException {
		throw LoadError.unexpectedError(
                         new Exception("Method not implemented"));
	}

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementatio  is provided , an exception is thrown if used.  
     *
     * @see java.sql.Blob 
     */
	public long position(byte pattern[], long start) throws SQLException
	{
		throw LoadError.unexpectedError(
                        new Exception("Method not implemented"));
	}

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided , an exception is thrown if used.  
     *
     * @see java.sql.Blob
     */
	public long position(Blob pattern, long start) throws SQLException {
		throw LoadError.unexpectedError(
                            new Exception("Method not implemented"));
	}


    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation  is provided , an exception is thrown if used.  
     *
     * @see java.sql.Blob
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
		throw LoadError.unexpectedError(new Exception("Method not implemented"));
	}

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation  is provided , an exception is thrown if used.  
     *
     * @see java.sql.Blob
     */
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException
	{
		throw LoadError.unexpectedError(
                        new Exception("Method not implemented"));
	}

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation  is provided , an exception is thrown if used.  
     *
     * @see java.sql.Blob
     */
    public java.io.OutputStream setBinaryStream(long pos) throws SQLException 
	{
		throw LoadError.unexpectedError(
                         new Exception("Method not implemented"));
	}

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation  is provided , an exception is thrown if used.  
     *
     * @see java.sql.Blob
     */
    public void truncate(long len) throws SQLException {
		throw LoadError.unexpectedError(
                        new Exception("Method not implemented"));
	}

//    @Override
    public void free() throws SQLException{
        throw Util.notImplemented("free");
    }

//    @Override
    public InputStream getBinaryStream(long pos,long length) throws SQLException{
        throw Util.notImplemented("getBinaryStream");
    }
}


