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

package com.splicemachine.db.client.am;



// Methods implemented by the common Statement class to handle
// certain events that may originate from the material or common layers.
//
// Reply implementations may update statement state via this interface.
//

public interface StatementCallbackInterface extends UnitOfWorkListener {
    // A query has been opened on the server.
    void completeOpenQuery(Sqlca sqlca, ResultSet resultSet) throws DisconnectException;

    void completeExecuteCallOpenQuery(Sqlca sqlca, ResultSet resultSet, ColumnMetaData resultSetMetaData, Section generatedSection);

    // Chains a warning onto the statement.
    void accumulateWarning(SqlWarning e);

    void completePrepare(Sqlca sqlca);

    void completePrepareDescribeOutput(ColumnMetaData columnMetaData, Sqlca sqlca);

    void completeExecuteImmediate(Sqlca sqlca);

    void completeExecuteSetStatement(Sqlca sqlca);


    void completeExecute(Sqlca sqlca);

    void completeExecuteCall(Sqlca sqlca, Cursor params, ResultSet[] resultSets);

    void completeExecuteCall(Sqlca sqlca, Cursor params);

    int completeSqlca(Sqlca sqlca);

    ConnectionCallbackInterface getConnectionCallbackInterface();

    ColumnMetaData getGuessedResultSetMetaData();
}
