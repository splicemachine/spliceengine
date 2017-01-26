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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.io.StorageFile;

import java.io.InputStream;

/**
	Management of file resources within	a database. Suitable for jar
	files, images etc.

	<P>A file resource is identified by the pair (name,generationId).
	Name is an arbitrary String supplied by the caller. GenerationId
	is a non-repeating sequence number constructed by the database.
	Within a database a	(name,generationId) pair uniquely identifies
	a version of a file resource for all time. Newer generation
	numbers reflect newer versions of the file.

	<P>A database supports the concept of a designated current version
	of a fileResource. The management of the current version is
	transactional. The following rules apply
	<OL>
	<LI>Adding a FileResource makes the added version the current
	version
	<LI>Removing a FileResource removes the current version of the
	resource. After this operation the database holds no current
	version of the FileResoure.
	<LI>Replacing a FileResource removes the current version of the
	resource.
	</OL>
	
	<P>For the benefit of replication, a database optionally retains 
	historic versions of stored files. These old versions are
	useful when processing old transactions in the stage. 
*/
public interface FileResource {

    /**
       The name of the jar directory
    */
    public static final String JAR_DIRECTORY_NAME = "jar";

	/**
	  Add a file resource, copying from the input stream.
	  
	  The InputStream will be closed by this method.
	  @param name the name of the file resource.
	  @param source an input stream for reading the content of
	         the file resource.
	  @return the generationId for the file resource. This
	  quantity increases when you replace the file resource.

	  @exception StandardException some error occured.
	*/
	public long add(String name,InputStream source)
		throws StandardException;

	/**
	  Remove the current generation of a file resource from
	  the database.

	  @param name the name of the fileResource to remove.
	  
	  @exception StandardException some error occured.
	  */
	public void remove(String name, long currentGenerationId)
		throws StandardException;

    /**
     * During hard upgrade to >= 10.9, remove a jar directory (at post-commit 
     * time) from the database.
     * @param f
     * @exception standard error policy
     */
    public void removeJarDir(String f) throws StandardException;
    
	/**
	  Replace a file resource with a new version.

	  <P>The InputStream will be closed by this method.

	  @param name the name of the file resource.
	  @param source an input stream for reading the content of
	  the file resource.
	  @return the generationId for the new 'current' version of the
	          file resource. 
	  @exception StandardException some error occured.
	*/
	public long replace(String name, long currentGenerationId, InputStream source)
		throws StandardException;

	/**
	  Get the StorageFile for a file resource.
	  
	  @param name The name of the fileResource
	  @param generationId the generationId of the fileResource
	  
	  @return A StorageFile object representing the file.
	  */
	public StorageFile getAsFile(String name, long generationId);

    /**
     * @return the separator character to be used in file names.
     */
    public char getSeparatorChar();
}
