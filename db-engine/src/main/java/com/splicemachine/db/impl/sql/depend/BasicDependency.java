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

package com.splicemachine.db.impl.sql.depend;

import com.splicemachine.db.iapi.sql.depend.Dependency;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;

import com.splicemachine.db.catalog.UUID;

import java.lang.ref.WeakReference;

/**
	A dependency represents a reliance of the dependent on
	the provider for some information the dependent contains
	or uses.  In Language, the usual case is a prepared statement
	using information about a schema object in its executable form.
	It needs to be notified if the schema object changes, so that
	it can recompile against the new information.
 */
class BasicDependency implements Dependency {

	//
	// Dependency interface
	//

	/**
		return the provider's key for this dependency.
		@return the provider' key for this dependency
	 */
	public UUID getProviderKey() {
		return provider.getObjectID();
	}

	/**
		return the provider for this dependency.
		@return the provider for this dependency
	 */
	public Provider getProvider() {
		return provider;
	}

	/**
		return the dependent for this dependency.
		@return the dependent for this dependency
	 */
	public Dependent getDependent() {
		return dependent.get();
	}

	//
	// class interface
	//
	BasicDependency(Dependent d, Provider p) {
		dependent = new WeakReference<Dependent>(d);
		provider = p;
	}

	//
	// class implementation
	//
	private final Provider	provider;
	private final WeakReference<Dependent>	dependent;
}
