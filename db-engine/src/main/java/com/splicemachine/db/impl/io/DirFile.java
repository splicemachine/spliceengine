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

package com.splicemachine.db.impl.io;

import com.splicemachine.db.io.StorageFile;
import com.splicemachine.db.io.StorageRandomAccessFile;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FileUtil;
import com.splicemachine.db.iapi.util.InterruptStatus;

/**
 * This class provides a disk based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */
class DirFile extends File implements StorageFile
{

    /**
     * Construct a DirFile from a path name.
     *
     * @param path The path name.
     */
    DirFile( String path)
    {
        super( path);
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile( String directoryName, String fileName)
    {
        super( directoryName, fileName);
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile( DirFile directoryName, String fileName)
    {
        super( (File) directoryName, fileName);
    }

    /**
     * Get the name of the parent directory if this name includes a parent.
     *
     * @return An StorageFile denoting the parent directory of this StorageFile, if it has a parent, null if
     *         it does not have a parent.
     */
    public StorageFile getParentDir()
    {
        String parent = getParent();
        if( parent == null)
            return null;
        return new DirFile( parent);
    }
    
    /**
     * Creates an output stream from a file name.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public OutputStream getOutputStream( ) throws FileNotFoundException
    {
        boolean exists = exists();
        OutputStream result = new FileOutputStream(this);

        if (!exists) {
            FileUtil.limitAccessToOwner(this);
        }

        return result;
    }
    
    /**
     * Creates an output stream from a file name.
     *
     * @param append If true then data will be appended to the end of the file, if it already exists.
     *               If false and a normal file already exists with this name the file will first be truncated
     *               to zero length.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public OutputStream getOutputStream( final boolean append) throws FileNotFoundException
    {
        boolean exists = exists();
        OutputStream result = new FileOutputStream( getPath(), append);

        if (!exists) {
            FileUtil.limitAccessToOwner(this);
        }

        return result;
    }

    /**
     * Creates an input stream from a file name.
     *
     * @return an input stream suitable for reading from the file.
     *
     * @exception FileNotFoundException if the file is not found.
     */
    public InputStream getInputStream( ) throws FileNotFoundException
    {
        return new FileInputStream( (File) this);
    }

    /**
     * Get an exclusive lock. This is used to ensure that two or more JVMs do not open the same database
     * at the same time.
     *
     *
     * @return EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE if the lock cannot be acquired because it is already held.<br>
     *    EXCLUSIVE_FILE_LOCK if the lock was successfully acquired.<br>
     *    NO_FILE_LOCK_SUPPORT if the system does not support exclusive locks.<br>
     */
    public synchronized int getExclusiveFileLock() throws StandardException
	{
		if (exists())
		{
			delete();
		}
		try
        {
			//Just create an empty file
			RandomAccessFile lockFileOpen = new RandomAccessFile( (File) this, "rw");
            limitAccessToOwner();
			lockFileOpen.getFD().sync( );
			lockFileOpen.close();
		}catch(IOException ioe)
		{
			// do nothing - it may be read only medium, who knows what the
			// problem is
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT(
                    "Unable to create Exclusive Lock File " + getPath(), ioe);
			}
		}
		
		return NO_FILE_LOCK_SUPPORT;
	} // end of getExclusiveFileLock

	/**
     * Release the resource associated with an earlier acquired exclusive lock
     *
     * @see #getExclusiveFileLock
     */
	public synchronized void releaseExclusiveFileLock()
	{
		if( exists())
		{
			delete(); 
		}
	} // End of releaseExclusiveFileLock

    /**
     * Get a random access (read/write) file.
     *
     * @param mode "r", "rw", "rws", or "rwd". The "rws" and "rwd" modes specify
     *             that the data is to be written to persistent store, consistent with the
     *             java.io.RandomAccessFile class ("synchronized" with the persistent
     *             storage, in the file system meaning of the word "synchronized").  However
     *             the implementation is not required to implement the "rws" or "rwd"
     *             modes. The implementation may treat "rws" and "rwd" as "rw". It is up to
     *             the user of this interface to call the StorageRandomAccessFile.sync
     *             method. If the "rws" or "rwd" modes are supported and the
     *             RandomAccessFile was opened in "rws" or "rwd" mode then the
     *             implementation of StorageRandomAccessFile.sync need not do anything.
     *
     * @return an object that can be used for random access to the file.
     *
     * @exception IllegalArgumentException if the mode argument is not equal to one of "r", "rw".
     * @exception FileNotFoundException if the file exists but is a directory rather than a regular
     *              file, or cannot be opened or created for any other reason .
     */
    public StorageRandomAccessFile getRandomAccessFile( String mode) throws FileNotFoundException
    {
        // Assume that modes "rws" and "rwd" are not supported.
        if( "rws".equals( mode) || "rwd".equals( mode))
            mode = "rw";
        return new DirRandomAccessFile( (File) this, mode);
    } // end of getRandomAccessFile

    /**
     * Rename the file denoted by this name. 
     *
     * Note that StorageFile objects are immutable. This method renames the 
     * underlying file, it does not change this StorageFile object. The 
     * StorageFile object denotes the same name as before, however the exists()
     * method will return false after the renameTo method executes successfully.
     *
     * <p>
     * It is not specified whether this method will succeed if a file 
     * already exists under the new name.
     *
     * @param newName the new name.
     *
     * @return <b>true</b> if the rename succeeded, <b>false</b> if not.
     */
    public boolean renameTo( StorageFile newName)
    {
        boolean rename_status = super.renameTo( (File) newName);
        int     retry_count   = 1;

        while (!rename_status && (retry_count <= 5))
        {
            // retry operation, hoping a temporary I/O resource issue is 
            // causing the failure.

            try
            {
                Thread.sleep(1000 * retry_count);
            }
            catch (InterruptedException ie)
            {
                // This thread received an interrupt as well, make a note.
                InterruptStatus.setInterrupted();
            }

            rename_status = super.renameTo((File) newName);

            retry_count++;
        }

        return(rename_status);
    }

    /**
     * Deletes the named file and, if it is a directory, all the files and directories it contains.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean deleteAll()
    {
        if( !exists())
            return false;
        if( isDirectory())
        {
            String[] childList = super.list();
            String parentName = getPath();
            for (String aChildList : childList) {
                if (aChildList.equals(".") || aChildList.equals(".."))
                    continue;
                DirFile child = new DirFile(parentName, aChildList);
                if (!child.deleteAll())
                    return false;
            }
        }
        return delete();
    } // end of deleteAll

	/**
	 * @see com.splicemachine.db.io.StorageFile#getURL()
	 */
	public URL getURL() throws MalformedURLException {
		
		return toURL();
	}

    public void limitAccessToOwner() {
        FileUtil.limitAccessToOwner(this);
    }
}
