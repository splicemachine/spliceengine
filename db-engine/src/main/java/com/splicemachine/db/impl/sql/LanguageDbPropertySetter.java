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

package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.io.Serializable;
import java.util.Dictionary;

/**
 * A class to handle setting language database properties
 */
public class LanguageDbPropertySetter implements PropertySetCallback
{
	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}
	/** @exception StandardException Thrown on error. */
	public boolean validate
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) throws StandardException 
	{
        // Can't change the dictionary version manually. That could make the database
        // unbootable. See DERBY-5838.
		if ( key.trim().equals( DataDictionary.CORE_DATA_DICTIONARY_VERSION ) )
		{
            throw StandardException.newException( SQLState.PROPERTY_UNSUPPORTED_CHANGE, key, value );
        }
        
		// Disallow changing sqlAuthorization from true to false or null after
		// switching to Standard authorization
		if (key.trim().equals(Property.SQL_AUTHORIZATION_PROPERTY))
		{
			LanguageConnectionContext lcc = (LanguageConnectionContext)
					ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

			if (lcc.usesSqlAuthorization() && !Boolean.valueOf((String) value))
				throw StandardException.newException(SQLState.PROPERTY_UNSUPPORTED_CHANGE,
					key, value);
		}

		if (key.equals(Property.LANGUAGE_STALE_PLAN_CHECK_INTERVAL)) {
			PropertyUtil.intPropertyValue(
						Property.LANGUAGE_STALE_PLAN_CHECK_INTERVAL,
						value,
						Property.MIN_LANGUAGE_STALE_PLAN_CHECK_INTERVAL,
						Integer.MAX_VALUE,
						Property.DEFAULT_LANGUAGE_STALE_PLAN_CHECK_INTERVAL
						);
			return true;
		}

		return false;
	}

	public Serviceable apply
	(
		String			key,
		Serializable	value,
		Dictionary		p,
		TransactionController tc
	) 
	{
		return null;
	}

 	public Serializable map
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) 
	{
		return null;
	}
}
