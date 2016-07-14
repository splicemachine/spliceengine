/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.sql.dictionary;

import	com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * This is the superclass of all Descriptors. Users of DataDictionary should use
 * the specific descriptor.
 *
 */

public class TupleDescriptor
{
	//////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	//////////////////////////////////////////////////////////////////


	//////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	//////////////////////////////////////////////////////////////////

	public     DataDictionary      dataDictionary;

	//////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	//////////////////////////////////////////////////////////////////

	public	TupleDescriptor() {}

	public TupleDescriptor(DataDictionary dataDictionary) {
		this.dataDictionary = dataDictionary;
	}

	public DataDictionary getDataDictionary() {
		return dataDictionary;
	}

	protected void setDataDictionary(DataDictionary dd) {
		dataDictionary = dd;
	}

	/**
	 * Is this provider persistent?  A stored dependency will be required
	 * if both the dependent and provider are persistent.
	 *
	 * @return boolean              Whether or not this provider is persistent.
	 */
	public boolean isPersistent() {
		return true;
	}


	//////////////////////////////////////////////////////////////////
	//
	//	BEHAVIOR. These are only used by Replication!!
	//
	//////////////////////////////////////////////////////////////////


	DependableFinder getDependableFinder(int formatId) {
		return dataDictionary.getDependableFinder(formatId);
	}

	DependableFinder getColumnDependableFinder(int formatId, byte[] columnBitMap) {
		return dataDictionary.getColumnDependableFinder(formatId, columnBitMap);
	}
	
	/** Each descriptor must identify itself with its type; i.e index, check
	 * constraint whatever.
	 */
	public String getDescriptorType() {
		if (SanityManager.DEBUG) {SanityManager.NOTREACHED(); }
		return null; 
	}
	/* each descriptor has a name
	 */
	public String getDescriptorName() {
		if (SanityManager.DEBUG) {SanityManager.NOTREACHED(); }
		return null; 
	}
}
