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

package com.splicemachine.db.impl.services.monitor;

import com.splicemachine.db.iapi.services.monitor.PersistentService;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.util.Properties;
import java.util.Hashtable;
import com.splicemachine.db.io.WritableStorageFactory;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.PassThroughException;
import com.splicemachine.db.iapi.reference.Property;

/**
*/
public class UpdateServiceProperties extends Properties {

	private PersistentService serviceType;
	private String serviceName;
    private volatile WritableStorageFactory storageFactory;
    
	/*
	Fix for bug 3668: Following would allow user to change properties while in the session
	in which the database was created.
	While the database is being created, serviceBooted would be false. What that means
	is, don't save changes into services.properties file from here until the database
	is created. Instead, let BaseMonitor save the properties at the end of the database
  creation and also set serviceBooted to true at that point. From then on, the
  services.properties file updates will be made here.
	*/
	private boolean serviceBooted;

	public UpdateServiceProperties(PersistentService serviceType, String serviceName,
	Properties actualSet, boolean serviceBooted) {
		super(actualSet);
		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.serviceBooted = serviceBooted;
	}

	//look at the comments for serviceBooted at the top to understand this.
	public void setServiceBooted() {
		serviceBooted = true;
	}

    public void setStorageFactory( WritableStorageFactory storageFactory)
    {
        this.storageFactory = storageFactory;
    }

    public WritableStorageFactory getStorageFactory()
    {
        return storageFactory;
    }
    
	/*
	** Methods of Hashtable (overridden)
	*/

	/**	
		Put the key-value pair in the Properties set and
		mark this set as modified.

		@see Hashtable#put
	*/
	public Object put(Object key, Object value) {
		Object ref = defaults.put(key, value);
		if (!((String) key).startsWith(Property.PROPERTY_RUNTIME_PREFIX))
			update();
		return ref;
	}

	/**	
		Remove the key-value pair from the Properties set and
		mark this set as modified.

		@see Hashtable#remove
	*/
	public Object remove(Object key) {
		Object ref = defaults.remove(key);
		if ((ref != null) &&
			(!((String) key).startsWith(Property.PROPERTY_RUNTIME_PREFIX)))
			update();
		return ref;
	}

	/**
	   Saves the service properties to the disk.
	 */
	public void saveServiceProperties()
	{
        if( SanityManager.DEBUG)
            SanityManager.ASSERT( storageFactory != null,
                                  "UpdateServiceProperties.saveServiceProperties() called before storageFactory set.");
		try{
			serviceType.saveServiceProperties(serviceName, storageFactory,
					BaseMonitor.removeRuntimeProperties(defaults), false);
		} catch (StandardException mse) {
			throw new PassThroughException(mse);
		}
	}

	/*
	** Class specific methods.
	*/

	private void update() {

		try {
			//look at the comments for serviceBooted at the top to understand this if.
			if (serviceBooted)
				serviceType.saveServiceProperties(serviceName, storageFactory,
					BaseMonitor.removeRuntimeProperties(defaults), true);
		} catch (StandardException mse) {
			throw new PassThroughException(mse);
		}
	}

}
