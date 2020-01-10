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

import com.splicemachine.db.client.am.ColumnMetaData;
import com.splicemachine.db.client.am.Section;
import com.splicemachine.db.client.am.SqlException;

// In general, all required data is passed.
// In addition, Material Statement objects are passed for convenient access to any material statement caches.
// Implementations of this interface should not dereference common layer Statement state, as it is passed in,
// but may dereference material layer Statement state if necessary for performance.

public interface StatementRequestInterface {
    void writeExecuteImmediate(NetStatement materialStatement,
                               String sql,
                               Section section) throws SqlException;

    void writePrepareDescribeOutput(NetStatement materialStatement,
                                    String sql,
                                    Section section) throws SqlException;

    void writePrepare(NetStatement materialStatement,
                      String sql,
                      Section section) throws SqlException;

    void writeOpenQuery(NetStatement materialStatement,
                        Section section,
                        int fetchSize,
                        int resultSetType) throws SqlException;

    void writeExecute(NetPreparedStatement materialPreparedStatement,
                      Section section,
                      com.splicemachine.db.client.am.ColumnMetaData parameterMetaData,
                      Object[] inputs,
                      int numInputColumns,
                      boolean outputExpected,
                      // This is a hint to the material layer that more write commands will follow.
                      // It is ignored by the driver in all cases except when blob data is written,
                      // in which case this boolean is used to optimize the implementation.
                      // Otherwise we wouldn't be able to chain after blob data is sent.
                      // If we could always chain a no-op DDM after every execute that writes blobs
                      // then we could just always set the chaining flag to on for blob send data
                      boolean chainedWritesFollowingSetLob) throws SqlException;


    void writeExecuteBatch(NetPreparedStatement materialPreparedStatement,
                      Section section,
                      com.splicemachine.db.client.am.ColumnMetaData parameterMetaData,
                      Object[] inputs,
                      int numInputColumns,
                      boolean outputExpected,
                      // This is a hint to the material layer that more write commands will follow.
                      // It is ignored by the driver in all cases except when blob data is written,
                      // in which case this boolean is used to optimize the implementation.
                      // Otherwise we wouldn't be able to chain after blob data is sent.
                      // If we could always chain a no-op DDM after every execute that writes blobs
                      // then we could just always set the chaining flag to on for blob send data
                      boolean chainedWritesFollowingSetLob) throws SqlException;

    void writeOpenQuery(NetPreparedStatement materialPreparedStatement,
                        Section section,
                        int fetchSize,
                        int resultSetType,
                        int numInputColumns,
                        ColumnMetaData parameterMetaData,
                        Object[] inputs) throws SqlException;

    void writeDescribeInput(NetPreparedStatement materialPreparedStatement,
                            Section section) throws SqlException;

    void writeDescribeOutput(NetPreparedStatement materialPreparedStatement,
                             Section section) throws SqlException;

    void writeExecuteCall(NetStatement materialStatement,
                          boolean outputExpected,
                          String procedureName,
                          Section section,
                          int fetchSize,
                          boolean suppressResultSets, // set to true for batched calls
                          int resultSetType,
                          ColumnMetaData parameterMetaData,
                          Object[] inputs) throws SqlException;


    void writeSetSpecialRegister(Section section, java.util.ArrayList sqlsttList) throws SqlException;
}
