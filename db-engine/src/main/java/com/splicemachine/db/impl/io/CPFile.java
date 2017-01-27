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

import java.io.InputStream;

import java.io.FileNotFoundException;
import java.net.URL;

/**
 * This class provides a class path based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the classpath subsubprotocol.
 */
class CPFile extends InputStreamFile
{

    private final CPStorageFactory storageFactory;
 
    CPFile( CPStorageFactory storageFactory, String path)
    {
        super( storageFactory, path);
        this.storageFactory = storageFactory;
    }

    CPFile( CPStorageFactory storageFactory, String parent, String name)
    {
        super( storageFactory, parent, name);
        this.storageFactory = storageFactory;
    }

    CPFile( CPFile dir, String name)
    {
        super( dir,name);
        this.storageFactory = dir.storageFactory;
    }

    private CPFile( CPStorageFactory storageFactory, String child, int pathLen)
    {
        super( storageFactory, child, pathLen);
        this.storageFactory = storageFactory;
    }

    /**
     * Tests whether the named file exists.
     *
     * @return <b>true</b> if the named file exists, <b>false</b> if not.
     */
    public boolean exists()
    {
    	return getURL() != null;
    } // end of exists

    /**
     * Get the parent of this file.
     *
     * @param pathLen the length of the parent's path name.
     */
    StorageFile getParentDir( int pathLen)
    {
        return new CPFile( storageFactory, path, pathLen);
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
    	//System.out.println("HERE FOR " + toString());
    	InputStream is = null;
    	ClassLoader cl = Thread.currentThread().getContextClassLoader();
    	if (cl != null)
    		is = cl.getResourceAsStream(path);
    	
       	// don't assume the context class loader is tied
    	// into the class loader that loaded this class.
    	if (is == null)
    	{
    		cl = getClass().getClassLoader();
    		// Javadoc indicates implementations can use
    		// null as a return from Class.getClassLoader()
    		// to indicate the system/bootstrap classloader.
    		if (cl != null)
    			is = cl.getResourceAsStream(path);
    		else
    			is = ClassLoader.getSystemResourceAsStream(path);
    	}
    	
    	if (is == null)
    		throw new FileNotFoundException(toString());
    	return is;
    	
    } // end of getInputStream
    
	/**
     * Return a URL for this file (resource).
     * 
     * @see com.splicemachine.db.io.StorageFile#getURL()
     */
    public URL getURL() {

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL myURL;
        if (cl != null) {
            myURL = cl.getResource(path);
            if (myURL != null)
                return myURL;
        }

        // don't assume the context class loader is tied
        // into the class loader that loaded this class.
        cl = getClass().getClassLoader();
        // Javadoc indicates implementations can use
        // null as a return from Class.getClassLoader()
        // to indicate the system/bootstrap classloader.
        if (cl != null) {
            return cl.getResource(path);
        } else {
            return ClassLoader.getSystemResource(path);
        }
    }
}
