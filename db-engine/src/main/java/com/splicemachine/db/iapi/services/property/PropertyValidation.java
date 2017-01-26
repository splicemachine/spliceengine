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

package com.splicemachine.db.iapi.services.property;

import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import java.io.Serializable;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

public class PropertyValidation implements PropertyFactory
{
	private Vector  notifyOnSet;

    /* Constructors for This class: */
	public PropertyValidation()
	{

	}

	public Serializable doValidateApplyAndMap(TransactionController tc,
											 String key, Serializable value,
											 Dictionary d, boolean dbOnlyProperty)
		 throws StandardException
	{
		Serializable mappedValue = null;
 		if (notifyOnSet != null) {
			synchronized (this) {

				for (int i = 0; i < notifyOnSet.size() ; i++) {
					PropertySetCallback psc = (PropertySetCallback) notifyOnSet.get(i);
					if (!psc.validate(key, value, d))
						continue;

					if (mappedValue == null)
 						mappedValue = psc.map(key, value, d);

					// if this property should not be used then
					// don't call apply. This depends on where
					// the old value comes from
					// SET_IN_JVM - property will not be used
					// SET_IN_DATABASE - propery will be used
					// SET_IN_APPLICATION - will become SET_IN_DATABASE
					// NOT_SET - will become SET_IN_DATABASE

					if (!dbOnlyProperty && key.startsWith("derby.")) {
						if (PropertyUtil.whereSet(key, d) == PropertyUtil.SET_IN_JVM)
							continue;
					}

					Serviceable s;
					if ((s = psc.apply(key,value,d,tc)) != null)
						((TransactionManager) tc).addPostCommitWork(s);
				}
			}
		}
		return mappedValue;
	}
	/**
	  Call the property set callbacks to map a proposed property value
	  to a value to save.
	  <P>
	  The caller must run this in a block synchronized on this
	  to serialize validations with changes to the set of
	  property callbacks
	  */
	public Serializable doMap(String key,
							 Serializable value,
							 Dictionary set)
		 throws StandardException
	{
		Serializable mappedValue = null;
 		if (notifyOnSet != null) {
			for (int i = 0; i < notifyOnSet.size() && mappedValue == null; i++) {
				PropertySetCallback psc = (PropertySetCallback) notifyOnSet.get(i);
				mappedValue = psc.map(key, value, set);
			}
		}

		if (mappedValue == null)
			return value;
		else
			return mappedValue;
	}

	public void validateSingleProperty(String key,
						  Serializable value,
						  Dictionary set)
		 throws StandardException
	{
		// RESOLVE: log device cannot be changed on the fly right now
		if (key.equals(Attribute.LOG_DEVICE))
        {
			throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CHANGE_LOGDEVICE);
        }

 		if (notifyOnSet != null) {
			for (int i = 0; i < notifyOnSet.size(); i++) {
				PropertySetCallback psc = (PropertySetCallback) notifyOnSet.get(i);
				psc.validate(key, value, set);
			}
		}
	}

	public synchronized void addPropertySetNotification(PropertySetCallback who){

		if (notifyOnSet == null)
			notifyOnSet = new Vector(1,1);
		notifyOnSet.add(who);

	}

	public synchronized void verifyPropertySet(Properties p,Properties ignore)
		 throws StandardException
	{
		for (Enumeration e = p.propertyNames(); e.hasMoreElements();)
		{
			String pn = (String)e.nextElement();
			//
			//Ignore the ones we are told to ignore.
			if (ignore.getProperty(pn) != null) continue;
			Serializable pv = p.getProperty(pn);
			validateSingleProperty(pn,pv,p);
		}
	}
}



