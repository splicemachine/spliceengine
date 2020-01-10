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

package com.splicemachine.db.iapi.services.property;

import java.util.Properties;

import java.io.Serializable;

import com.splicemachine.db.iapi.error.StandardException;

public interface PersistentSet
{
    /**
     * Gets a value for a stored property. The returned value will be:
	 *
	 * <OL>
	 * <LI> the de-serialized object associated with the key
	 *      using setProperty if such a value is defined or
	 * <LI> the default de-serialized object associated with
	 *      the key using setPropertyDefault if such a value
	 *      is defined or
	 * <LI> null
	 * </OL>
	 *      
     * <p>
     * The Store provides a transaction protected list of database properties.
     * Higher levels of the system can store and retrieve these properties
     * once Recovery has finished. Each property is a serializable object
     * and is stored/retrieved using a String key.
     * <p>
     *
     * @param key     The "key" of the property that is being requested.
     *
	 * @return object The requested object or null.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	Serializable getProperty(
			String key)
        throws StandardException;

    /**
     * Gets a default value for a stored property. The returned
	 * value will be:
	 *
	 * <OL>
	 * <LI> the default de-serialized object associated with
	 *      the key using setPropertyDefault if such a value
	 *      is defined or
	 * <LI> null
	 * </OL>
	 *      
     * <p>
     * The Store provides a transaction protected list of database properties.
     * Higher levels of the system can store and retrieve these properties
     * once Recovery has finished. Each property is a serializable object
     * and is stored/retrieved using a String key.
     * <p>
     *
     * @param key     The "key" of the property that is being requested.
     *
	 * @return object The requested object or null.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	Serializable getPropertyDefault(
			String key)
        throws StandardException;


    /**
     * Return true if the default property is visible. A default
	 * is visible as long as the property is not set.
     *
     * @param key     The "key" of the property that is being requested.
	 * @exception  StandardException  Standard exception policy.
     **/
	boolean propertyDefaultIsVisible(String key) throws StandardException;

    /**
     * Sets the Serializable object associated with a property key.
     * <p>
     * See the discussion of getProperty().
     * <p>
     * The value stored may be a Formatable object or a Serializable object
	 * whose class name starts with java.*. This stops arbitary objects being
	 * stored in the database by class name, which will cause problems in
	 * obfuscated/non-obfuscated systems.
     *
	 * @param	key		The key used to lookup this property.
	 * @param	value	The value to be associated with this key. If null, 
     *                  delete the property from the properties list.
	   @param   dbOnlyProperty True if property is only ever searched for int the database properties.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	void	setProperty(
			String key,
			Serializable value,
			boolean dbOnlyProperty)
        throws StandardException;

    /**
     * Sets the Serializable object default value associated with a property
	 * key.
     * <p>
     * See the discussion of getProperty().
     * <p>
     * The value stored may be a Formatable object or a Serializable object
	 * whose class name starts with java.*. This stops arbitary objects being
	 * stored in the database by class name, which will cause problems in
	 * obfuscated/non-obfuscated systems.
     *
	 * @param	key		The key used to lookup this propertyDefault.
	 * @param	value	The default value to be associated with this key. 
     *                  If null, delete the property default from the
	 *                  properties list.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	void	setPropertyDefault(
			String key,
			Serializable value)
        throws StandardException;

    /**
     * Get properties that can be stored in a java.util.Properties object.
     * <p>
	 * Get the sub-set of stored properties that can be stored in a 
     * java.util.Properties object. That is all the properties that have a
     * value of type java.lang.String.  Changes to this properties object are
     * not reflected in any persisent storage.
     * <p>
     * Code must use the setProperty() method call.
     *
	 * @return The sub-set of stored properties that can be stored in a 
     *         java.util.Propertes object.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	Properties getProperties()
        throws StandardException;
}
