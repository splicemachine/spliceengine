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
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
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



public interface MaterialPreparedStatement extends MaterialStatement {


    // ------------------------ abstract box car and callback methods --------------------------------

    public abstract void writeExecute_(Section section,
                                       ColumnMetaData parameterMetaData,
                                       Object[] inputs,
                                       int numInputColumns,
                                       boolean outputExpected,
                                       // This is a hint to the material layer that more write commands will follow.
                                       // It is ignored by the driver in all cases except when blob data is written,
                                       // in which case this boolean is used to optimize the implementation.
                                       // Otherwise we wouldn't be able to chain after blob data is sent.
                                       // Current servers have a restriction that blobs can only be chained with blobs
                                       // Can the blob code
                                       boolean chainedWritesFollowingSetLob) throws SqlException;


    public abstract void readExecute_() throws SqlException;

    public abstract void writeOpenQuery_(Section section,
                                         int fetchSize,
                                         int resultSetType,
                                         int numInputColumns,
                                         ColumnMetaData parameterMetaData,
                                         Object[] inputs) throws SqlException;

    public abstract void writeDescribeInput_(Section section) throws SqlException;

    public abstract void readDescribeInput_() throws SqlException;

    public abstract void writeDescribeOutput_(Section section) throws SqlException;

    public abstract void readDescribeOutput_() throws SqlException;
}
