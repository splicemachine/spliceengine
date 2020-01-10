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

package com.splicemachine.db.iapi.store.access;


/**

  Information that can be "compiled" and reused per transaction per 
  open operation.  This information is read only by the caller and
  written by user.  Likely information kept in this object is a set of
  scratch buffers which will be used by openScan() and thus must not be
  shared across multiple threads/openScan()'s/openConglomerate()'s.  The
  goal is to optimize repeated operations like btree inserts, by allowing a
  set of scratch buffers to be reused across a repeated execution of a statement
  like an insert/delete/update.

  This information is obtained from the getDynamicCompiledConglomInfo(conglomid)
  method call.  It can then be used in openConglomerate() and openScan() calls
  for increased performance.  The information is only valid until the next
  ddl operation is performed on the conglomerate.  It is up to the caller to
  provide an invalidation methodology.
  
  The dynamic info is a set of variables to be used in a given ScanController
  or ConglomerateController.  It can only be used in one controller at a time.
  It is up to the caller to insure the correct thread access to this info.  The
  type of info in this is a scratch template for btree traversal, other scratch
  variables for qualifier evaluation, ...

**/

public interface DynamicCompiledOpenConglomInfo
{
}
