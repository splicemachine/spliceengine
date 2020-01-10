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



public interface MaterialStatement {
    void writeExecuteImmediate_(String sql, Section section) throws SqlException;

    void readExecuteImmediate_() throws SqlException;

    // The sql parameter is supplied in the read method for drivers that
    // process all commands on the "read-side" and do little/nothing on the "write-side".
    // Drivers that follow the write/read paradigm (e.g. NET) will likely ignore the sql parameter.
    void readExecuteImmediateForBatch_(String sql) throws SqlException;

    void writePrepareDescribeOutput_(String sql, Section section) throws SqlException;

    void readPrepareDescribeOutput_() throws SqlException;

    void writeOpenQuery_(Section section,
                         int fetchSize,
                         int resultSetType) throws SqlException;

    void readOpenQuery_() throws SqlException;

    void writeExecuteCall_(boolean outputExpected,
                           String procedureName,
                           Section section,
                           int fetchSize,
                           boolean suppressResultSets, // for batch updates set to true, otherwise to false
                           int resultSetType,
                           ColumnMetaData parameterMetaData,
                           Object[] inputs) throws SqlException;

    void readExecuteCall_() throws SqlException;

    // Used for re-prepares across commit and other places as well
    void writePrepare_(String sql, Section section) throws SqlException;

    void readPrepare_() throws SqlException;

    void markClosedOnServer_();

    void writeSetSpecialRegister_(Section section, java.util.ArrayList sqlsttList) throws SqlException;

    void readSetSpecialRegister_() throws SqlException;

    void reset_();

}

