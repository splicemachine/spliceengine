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
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.net;

import com.splicemachine.db.client.am.Section;

// In general, required data is passed.
// In addition, ResultSet objects are passed for convenient access to any material result set caches.
// Implementations of this interface should not dereference common layer ResultSet state, as it is passed in,
// but may dereference material layer ResultSet state if necessary for performance.

public interface ResultSetRequestInterface {
    void writeFetch(NetResultSet resultSet,
                    Section section,
                    int fetchSize) throws com.splicemachine.db.client.am.SqlException;

    void writeScrollableFetch(NetResultSet resultSet,
                              Section section,
                              int fetchSize,
                              int orientation,
                              long rowToFetch,
                              boolean resetQueryBlocks) throws com.splicemachine.db.client.am.SqlException;

    void writePositioningFetch(NetResultSet resultSet,
                               Section section,
                               int orientation,
                               long rowToFetch) throws com.splicemachine.db.client.am.SqlException;

    void writeCursorClose(NetResultSet resultSet,
                          Section section) throws com.splicemachine.db.client.am.SqlException;

}
