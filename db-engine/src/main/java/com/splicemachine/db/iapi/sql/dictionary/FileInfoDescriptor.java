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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

/**
 * A Descriptor for a file that has been stored in the database.
 */
public final class  FileInfoDescriptor extends TupleDescriptor 
	implements Provider, UniqueSQLObjectDescriptor
{
	/** A type tho indicate the file is a jar file **/
	public static final int JAR_FILE_TYPE = 0;

	/** external interface to this class:
		<ol>
		<li>public long	getGenerationId();
		</ol>
	*/
	private final UUID id;
	private final SchemaDescriptor sd;
	private final String sqlName;
	private final long generationId;
	
	/**
	 * Constructor for a FileInfoDescriptor.
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param id        	The id for this file
	 * @param sd			The schema for this file.
	 * @param sqlName		The SQL name of this file.
	 * @param generationId  The generation id for the
	 *                      version of the file this describes.
	 */

	public FileInfoDescriptor(DataDictionary dataDictionary,
								 UUID id,
								 SchemaDescriptor sd,
								 String sqlName,
								 long generationId)
	{
		super( dataDictionary );

		if (SanityManager.DEBUG)
		{
			if (sd.getSchemaName() == null)
			{
				SanityManager.THROWASSERT("new FileInfoDescriptor() schema "+
					"name is null for FileInfo "+sqlName);
			}
		}
		this.id = id;
		this.sd = sd;
		this.sqlName = sqlName;
		this.generationId = generationId;
	}

	public SchemaDescriptor getSchemaDescriptor()
	{
		return sd;
	}

	public String getName()
	{
		return sqlName;
	}

	/**
	 * @see UniqueTupleDescriptor#getUUID
	 */
	public UUID	getUUID()
	{
		return id;
	}

	/**
	 * Gets the generationId for the current version of this file. The
	 * triple (schemaName,SQLName,generationId) are unique for the
	 * life of this database.
	 *
	 * @return	the generationId for this file
	 */
	public long getGenerationId()
	{
		return generationId;
	}

	//
	// Provider interface
	//

	/**		
	  @see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
	    return	getDependableFinder(StoredFormatIds.FILE_INFO_FINDER_V01_ID);
	}

	/**
	  @see Dependable#getObjectName
	 */
	public String getObjectName()
	{
		return sqlName;
	}

	/**
	  @see Dependable#getObjectID
	 */
	public UUID getObjectID()
	{
		return id;
	}

	/**
	  @see Dependable#getClassType
	 */
	public String getClassType()
	{
		return Dependable.FILE;
	}

	//
	// class interface
	//

	
	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType() { return "Jar file"; }

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return sqlName; }



}
