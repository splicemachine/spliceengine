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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.services.io.Storable;


/**

  Information that can be "compiled" once and then used over and over again
  at execution time.  This information is read only by both the caller and
  the user, thus can be shared by multiple threads/transactions once created.

  This information is obtained from the getStaticCompiledConglomInfo(conglomid)
  method call.  It can then be used in openConglomerate() and openScan() calls
  for increased performance.  The information is only valid until the next
  ddl operation is performed on the conglomerate.  It is up to the caller to
  provide an invalidation methodology.

  The static info would be valid until any ddl was executed on the conglomid,
  and would be up to the caller to throw away when that happened.  This ties in
  with what language already does for other invalidation of static info.  The
  type of info in this would be containerid and array of format id's from which
  templates can be created.  The info in this object is read only and can
  be shared among as many threads as necessary.

**/

public interface StaticCompiledOpenConglomInfo extends Storable
{
    /**
     * routine for internal use of store only.
     **/
    DataValueDescriptor  getConglom();
}
