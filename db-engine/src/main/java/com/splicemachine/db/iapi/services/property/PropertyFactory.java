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

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.Properties;
import java.io.Serializable;
import java.util.Dictionary;

/**
  Module interface for an Property validation.  

  <p>
  An PropertyFactory is typically obtained from the Monitor:
  <p>
  <blockquote><pre>
	// Get the current validation factory.
	PropertyFactory af;
	af = (PropertyFactory) Monitor.findServiceModule(this, com.splicemachine.db.iapi.reference.Module.PropertyFactory);
  </pre></blockquote>
**/

public interface PropertyFactory
{
    /**************************************************************************
     * methods that are Property related.
     **************************************************************************
     */

    /**
     * Add a callback for a change in any property value.
	 * <BR>
     * The callback is made in the context of the transaction making the change.
     *
     * @param who   which object is called
     **/
	void addPropertySetNotification(
			PropertySetCallback who);

    /**
     * Validate a Property set.
     * <p>
     * Validate a Property set by calling all the registered property set
     * notification functions with .
     *
	 * @param p Properties to validate.
	 * @param ignore Properties to not validate in p. Usefull for properties
	 *        that may not be set after boot. 
     *
	 * @exception StandardException Throws if p fails a check.
     **/
	void verifyPropertySet(
			Properties p,
			Properties ignore)
        throws StandardException;

	/**
	 * validation a single property
	 */
	void validateSingleProperty(String key,
								Serializable value,
								Dictionary set)
		throws StandardException;

	/**
	   
	 */
	Serializable doValidateApplyAndMap(TransactionController tc,
									   String key, Serializable value,
									   Dictionary d, boolean dbOnlyProperty)
		throws StandardException;


	/**
	  Call the property set callbacks to map a proposed property value
	  to a value to save.
	  <P>
	  The caller must run this in a block synchronized on this
	  to serialize validations with changes to the set of
	  property callbacks
	  */
	Serializable doMap(String key,
					   Serializable value,
					   Dictionary set)
		throws StandardException;
}
