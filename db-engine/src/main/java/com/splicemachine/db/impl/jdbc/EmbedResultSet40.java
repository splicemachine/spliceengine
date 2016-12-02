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

package com.splicemachine.db.impl.jdbc;

import com.splicemachine.db.iapi.sql.ResultSet;

import java.sql.SQLException;

/**
 * JDBC 4 specific methods that cannot be implemented in superclasses and
 * unimplemented JDBC 4 methods.
 * In general, the implementations should be pushed to the superclasses. This
 * is not possible if the methods use objects or features not available in the
 * Java version associated with the earlier JDBC version, since Derby classes
 * are compiled with the lowest possible Java version.
 */
public class EmbedResultSet40 extends EmbedResultSet20 {
    
    /** Creates a new instance of EmbedResultSet40 */
    public EmbedResultSet40(EmbedConnection conn,
        ResultSet resultsToWrap,
        boolean forMetaData,
        EmbedStatement stmt,
        boolean isAtomic)
        throws SQLException {
        
        super(conn, resultsToWrap, forMetaData, stmt, isAtomic);
    }
    

    



}
