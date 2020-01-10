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

package com.splicemachine.db.iapi.services.cache;

import com.splicemachine.db.iapi.error.StandardException;

/**
	Any object that implements this interface can be cached using the services of
	the CacheManager/CacheFactory. In addition to implementing this interface the
	class must be public and it must have a public no-arg constructor. This is because
	the cache manager will construct objects itself and then set their identity
	by calling the setIdentity method.
	<P>
	A Cacheable object has five states:
	<OL>
	<OL>
	<LI> No identity - The object is only accessable by the cache manager
	<LI> Identity, clean, unkept - The object has an identity, is clean but is only accessable by the cache manager
	<LI> Identity, clean, kept - The object has an identity, is clean, and is in use by one or more threads 
	<LI> Identity, kept, dirty - The object has an identity, is dirty, and is in use by one or more threads 
	<LI> Identity, unkept, dirty - The object has an identity, is dirty but is only accessable by the cache manager
	</OL>
	</OL>
	<BR>
	While the object is kept it is guaranteed
	not to change identity. While it is unkept no-one outside of the
	cache manager can have a reference to the object.
	The cache manager returns kept objects and they return to the unkept state
	when all the current users of the object have released it.
	<BR>
	It is required that the object can only move into a dirty state while it is kept.

	<BR> MT - Mutable : thread aware - Calls to Cacheable method must only be made by the
	cache manager or the object itself.

	@see CacheManager
	@see CacheFactory
	@see Class#newInstance
*/
public interface Cacheable  {

	/**
        Set the identity of the object.
        <p>
		Set the identity of the object to represent an item that already exists,
		e.g. an existing container.
		The object will be in the No Identity state,
		ie. it will have just been created or clearIdentity() was just called. 
		<BR>
		The object must copy the information out of key, not just store a reference to key.
		After this call the expression getIdentity().equals(key) must return true.
		<BR>
		If the class of the object needs to change (e.g. to support a different format)
		then the object should create a new object, call its initParameter() with the parameters
		the original object was called with, set its identity and return a reference to it. The cache
		manager will discard the reference to the old object. 
		<BR>
		If an exception is thrown the object must be left in the no-identity state.

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

		@return an object reference if the object can take on the identity, null otherwise.

		@exception StandardException Standard Derby Policy

		@see CacheManager#find

	*/
	Cacheable setIdentity(Object key) throws StandardException;

	/**
        Create a new item.
        <p>
		Create a new item and set the identity of the object to represent it.
		The object will be in the No Identity state,
		ie. it will have just been created or clearIdentity() was just called. 
		<BR>
		The object must copy the information out of key, not just store a reference to key
		if the key is not immutable.
		After this call the expression getIdentity().equals(key) must return true.
		<BR>
		<BR>
		If the class of the object needs to change (e.g. to support a different format)
		then the object should create a new object, call its initParameter() with the parameters
		the original object was called with, set its identity and return a reference to it. The cache
		manager will discard the reference to the old object. 
		<BR>
		If an exception is thrown the object must be left in the no-identity state.

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

		@return an object reference if the object can take on the identity, null otherwise.

		@exception StandardException If forCreate is true and the object cannot be created.

		@see CacheManager#create

	*/
	Cacheable createIdentity(Object key, Object createParameter) throws StandardException;


	/**
		Put the object into the No Identity state. 

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

	*/
	void clearIdentity();

	/**
		Get the identity of this object.

		<BR> MT - thread safe.

	*/
	Object getIdentity();


	/**
		Returns true of the object is dirty. 
		May be called when the object is kept or unkept.

		<BR> MT - thread safe 

	*/
	boolean isDirty();

	/**
		Clean the object.
		It is up to the object to ensure synchronization of the isDirty()
		and clean() method calls.
		<BR>
		If forRemove is true then the 
		object is being removed due to an explict remove request, in this case
		the cache manager will have called this method regardless of the
		state of the isDirty() 

		<BR>
		If an exception is thrown the object must be left in the clean state.

		<BR> MT - thread safe - Can be called at any time by the cache manager, it is the
		responsibility of the object implementing Cacheable to ensure any users of the
		object do not conflict with the clean call.

		@exception StandardException Standard Derby error policy.

	*/
	void clean(boolean forRemove) throws StandardException;
}

