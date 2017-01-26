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

package com.splicemachine.db.impl.store.access;

import com.splicemachine.db.iapi.services.cache.Cacheable;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;

/**
The CacheableConglomerate implements a single item in the cache used by
the Conglomerate directory to cache Conglomerates.  It is simply a wrapper
object for the conglomid and Conglomerate object that is read from the
Conglomerate Conglomerate.   It is a wrapper rather than extending 
the conglomerate implementations because we want to cache all conglomerate
implementatations: (ie. Heap, B2I, ...).

References to the Conglomerate objects cached by this wrapper will be handed
out to callers.  When this this object goes out of cache callers may still
have references to the Conglomerate objects, which we are counting on java
to garbage collect.  The Conglomerate Objects never change after they are
created.

**/

class CacheableConglomerate implements Cacheable
{
    private Long            conglomid;
    private Conglomerate    conglom;

    /* Constructor */
    CacheableConglomerate()
    {
    }

	/*
	** protected Methods of CacheableConglomerate:
	*/
    protected Conglomerate getConglom()
    {
        return(this.conglom);
    }

	/*
	** Methods of Cacheable:
	*/

	/**
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

		@see com.splicemachine.db.iapi.services.cache.CacheManager#find

	*/
	public Cacheable setIdentity(Object key) throws StandardException
    {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("not supported.");
		}

        return(null);
    }

	/**
     * Create a new item and set the identity of the object to represent it.
	 * The object will be in the No Identity state,
	 * ie. it will have just been created or clearIdentity() was just called. 
	 * <BR>
	 * The object must copy the information out of key, not just store a 
     * reference to key.  After this call the expression 
     * getIdentity().equals(key) must return true.
	 * <BR>
	 * If the class of the object needs to change (e.g. to support a different 
     * format) then the object should create a new object, call its 
     * initParameter() with the parameters the original object was called with,
     * set its identity and return a reference to it. The cache manager will 
     * discard the reference to the old object. 
	 * <BR>
	 * If an exception is thrown the object must be left in the no-identity 
     * state.
	 * <BR> MT - single thread required - Method must only be called be cache 
     * manager and the cache manager will guarantee only one thread can be 
     * calling it.
     *
	 * @return an object reference if the object can take on the identity, 
     * null otherwise.
     *
	 * @exception StandardException If forCreate is true and the object cannot 
     * be created.
     *
	 * @see com.splicemachine.db.iapi.services.cache.CacheManager#create
	 **/
	public Cacheable createIdentity(Object key, Object createParameter) 
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                key instanceof Long, "key is not instanceof Long");
            SanityManager.ASSERT(
                createParameter instanceof Conglomerate, 
                "createParameter is not instanceof Conglomerate");
        }

        this.conglomid = (Long) key;
        this.conglom   = ((Conglomerate) createParameter);

        return(this);
    }

	/**
		Put the object into the No Identity state. 

		<BR> MT - single thread required - Method must only be called be cache manager
		and the cache manager will guarantee only one thread can be calling it.

	*/
	public void clearIdentity()
    {
        this.conglomid = null;
        this.conglom   = null;
    }

	/**
		Get the identity of this object.

		<BR> MT - thread safe.

	*/
	public Object getIdentity()
    {
        return(this.conglomid);
    }


	/**
		Returns true of the object is dirty. Will only be called when the object is unkept.

		<BR> MT - thread safe 

	*/
	public boolean isDirty()
    {
        return(false);
    }

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
	public void clean(boolean forRemove) throws StandardException
    {
    }
}
