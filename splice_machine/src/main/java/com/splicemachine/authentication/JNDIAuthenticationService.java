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

package com.splicemachine.authentication;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.property.PropertyUtil;

import com.splicemachine.db.authentication.UserAuthenticator;

import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;
import com.splicemachine.derby.lifecycle.ManagerLoader;

import java.util.Properties;

/**
 * This is the JNDI Authentication Service base class.
 * <p>
 * It instantiates the JNDI authentication scheme defined by the user/
 * administrator. Derby supports LDAP JNDI providers.
 * <p>
 * The user can configure its own JNDI provider by setting the
 * system or database property db.authentication.provider .
 *
 */

public class JNDIAuthenticationService
	extends AuthenticationServiceBase {

    //
	// constructor
	//

	// call the super
	public JNDIAuthenticationService() {
		super();
	}

	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate the JNDI authentication service.
	 */
	public boolean canSupport(Properties properties) {

		if (!requireAuthentication(properties))
			return false;

		//
		// we check 2 things:
		//
		// - if db.connection.requireAuthentication system
		//   property is set to true.
		// - if db.authentication.provider is set to one
		// of the JNDI scheme we support (i.e. LDAP).
		//

        String authenticationProvider = PropertyUtil.getPropertyFromSet(
            properties,
            Property.AUTHENTICATION_PROVIDER_PARAMETER);

        return (authenticationProvider != null) &&
            (StringUtil.SQLEqualsIgnoreCase(authenticationProvider,
                                            Property.AUTHENTICATION_PROVIDER_LDAP));

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
		// told to.

		// Set ourselves as being ready and loading the proper
		// authentication scheme for this service
		UserAuthenticator aJNDIAuthscheme;

		// we're dealing with LDAP
		aJNDIAuthscheme = ManagerLoader.load().getAuthenticationManager(this, properties);
		this.setAuthenticationService(aJNDIAuthscheme);
	}
}
