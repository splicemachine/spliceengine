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

package com.splicemachine.db.iapi.services.monitor;

import java.util.Properties;

/**
	Allows a module to check its environment
	before it is selected as an implementation.
*/

public interface ModuleSupportable {

	/**
		See if this implementation can support any attributes that are listed in properties.
		This call may be made on a newly created instance before the
		boot() method has been called, or after the boot method has
		been called for a running module.
		<P>
		The module can check for attributes in the properties to
		see if it can fulfill the required behaviour. E.g. the raw
		store may define an attribute called RawStore.Recoverable.
		If a temporary raw store is required the property RawStore.recoverable=false
		would be added to the properties before calling bootServiceModule. If a
		raw store cannot support this attribute its canSupport method would
		return null. Also see the Monitor class's prologue to see how the
		identifier is used in looking up properties.
		<BR><B>Actually a better way maybe to have properties of the form
		RawStore.Attributes.mandatory=recoverable,smallfootprint and
		RawStore.Attributes.requested=oltp,fast
		</B>

		@return true if this instance can be used, false otherwise.
	*/
	boolean canSupport(Properties properties);

}
