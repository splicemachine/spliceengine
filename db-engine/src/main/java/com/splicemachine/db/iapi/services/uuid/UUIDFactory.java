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

package com.splicemachine.db.iapi.services.uuid;

import com.splicemachine.db.catalog.UUID;

/*
	Internal comment (not for user documentation):
  Although this is an abstract interface, I believe that the
  underlying implementation of UUID will have to be DCE UUID.
  This is because the string versions of UUIDs get stored in
  the source code.  In other words, no matter what implementation
  is used for UUIDs, strings that look like this
  <blockquote><pre>
	E4900B90-DA0E-11d0-BAFE-0060973F0942
  </blockquote></pre>
  will always have to be turned into universally unique objects
  by the recreateUUID method
 */
/**
	
  Generates and recreates unique identifiers.
  
  An example of such an identifier is:
  <blockquote><pre>
	E4900B90-DA0E-11d0-BAFE-0060973F0942
  </blockquote></pre>
  These resemble DCE UUIDs, but use a different implementation.
  <P>
  The string format is designed to be the same as the string
  format produced by Microsoft's UUIDGEN program, although at
  present the bit fields are probably not the same.
  
 **/
public interface UUIDFactory 
{
	/**
	  Create a new UUID.  The resulting object is guaranteed
	  to be unique "across space and time".
	  @return		The UUID.
	**/
	UUID createUUID();

	/**
	  Recreate a UUID from a string produced by UUID.toString.
	  @return		The UUID.
	**/
	UUID recreateUUID(String uuidstring);
}

