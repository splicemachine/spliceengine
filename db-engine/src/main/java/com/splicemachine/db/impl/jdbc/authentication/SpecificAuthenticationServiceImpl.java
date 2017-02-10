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

package com.splicemachine.db.impl.jdbc.authentication;

import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.authentication.UserAuthenticator;

import com.splicemachine.db.iapi.services.property.PropertyUtil;

import java.util.Properties;

/**
 * This authentication service is a specific/user defined User authentication
 * level support.
 * <p>
 * It calls the specific User authentication scheme defined by the user/
 * administrator.
 *
 */
public class SpecificAuthenticationServiceImpl
	extends AuthenticationServiceBase {

	private String specificAuthenticationScheme;

	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate this authentication service.
	 */
	public boolean canSupport(Properties properties) {

		//
		// we check 2 things:
		// - if db.connection.requireAuthentication system
		//   property is set to true.
		// - if db.authentication.provider is set and is not equal
		//	 to LDAP or BUILTIN.
		//
		// and in that case we are the authentication service that should
		// be run.
		//
		if (!requireAuthentication(properties))
			return false;

        //
        // Don't treat the NATIVE authentication specification as a user-supplied
        // class which should be instantiated.
        //
        if (  PropertyUtil.nativeAuthenticationEnabled( properties ) ) { return false; }

		specificAuthenticationScheme = PropertyUtil.getPropertyFromSet(
					properties,
					Property.AUTHENTICATION_PROVIDER_PARAMETER);
		if (
			 ((specificAuthenticationScheme != null) &&
			  (specificAuthenticationScheme.length() != 0) &&

			  (!((StringUtil.SQLEqualsIgnoreCase(specificAuthenticationScheme,
					  Property.AUTHENTICATION_PROVIDER_BUILTIN)) ||
			  (specificAuthenticationScheme.equalsIgnoreCase(
                                                             Property.AUTHENTICATION_PROVIDER_LDAP))  ))))
			return true;
		else
			return false;
	}

	/**
	 * @see com.splicemachine.db.iapi.services.monitor.ModuleControl#boot
	 * @exception StandardException upon failure to load/boot the expected
	 * authentication service.
	 */
	public void boot(boolean create, Properties properties)
	  throws StandardException {

		// We need authentication
		// setAuthentication(true);

		// we call the super in case there is anything to get initialized.
		super.boot(create, properties);

		// We must retrieve and load the authentication scheme that we were
		// told to. The class loader will report an exception if it could not
		// find the class in the classpath.
		//
		// We must then make sure that the ImplementationScheme loaded,
		// implements the published UserAuthenticator interface we
		// provide.
		//

		Throwable t;
		try {

			Class sasClass = Class.forName(specificAuthenticationScheme);
			if (!UserAuthenticator.class.isAssignableFrom(sasClass)) {
				throw StandardException.newException(SQLState.AUTHENTICATION_NOT_IMPLEMENTED,
					specificAuthenticationScheme, "com.splicemachine.db.authentication.UserAuthenticator");
			}

			UserAuthenticator aScheme = (UserAuthenticator) sasClass.newInstance();

			// Set ourselves as being ready and loading the proper
			// authentication scheme for this service
			//
			this.setAuthenticationService(aScheme);

			return;

		} catch (ClassNotFoundException cnfe) {
			t = cnfe;
		} catch (InstantiationException ie) {
			t = ie;
		} catch (IllegalAccessException iae) {
			t = iae;
		}
        
        String  detail = t.getClass().getName() + ": " + t.getMessage();
		throw StandardException.newException
            ( SQLState.AUTHENTICATION_SCHEME_ERROR, specificAuthenticationScheme, detail );
	}
}
