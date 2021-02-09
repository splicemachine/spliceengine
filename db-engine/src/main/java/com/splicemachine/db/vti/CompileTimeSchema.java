/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.vti;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * This interface provides information whether a VTI has a compile-time result set, if a VTI has
 * such a result set then query compilation is able to infer the schema of the result set of the
 * VTI even if the user did not specify it explicitly in the query.
 *
 * Example
 * =======
 * Here is an example that assumes we have a VTI called com.splicemachine.db.vti.XYZ which has a
 * result set of type (varchar(20), timestamp) and accepts a varchar(12) parameter, running such
 * query:
 *
 *  `select * from new com.splicemachine.db.vti.XYZ('foo')`
 *
 * works as expected provided that XYZ implements this interface.
 *
 * How does it work?
 * =================
 *
 * directly after parsing and building `FromVTI` node we check if `tableProperties` (i.e. user-
 * defined schema) is defined, if not, then we use reflection to examine the invoked VTI, in this
 * example it would be com.splicemachine.db.vti.XYZ, then we invoke `schemaKnownAtCompileTime` to see
 * if the VTI has a static compile-time schema, if so, we retrieve it by calling `getMetaData` and
 * we perform some transformation before we proceed with binding and optimization.
 *
 * How to use it?
 * ==============
 *
 * If your VTI returns a fixed-type result set, you should override the default implementation of
 * these methods and provide information about your schema, that's all, not implementing this interface
 * is also fine, it just means that the user will always have to provide the schema explicitly when
 * invoking your VTI, so it makes sense to leave the method untouched e.g. if the result set returned
 * from your VTI is dynamic.
 *
 * @note the column names in your schemas should be uniquely defined.
 */
public interface CompileTimeSchema {

    static ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("not supported");
    }

    static boolean schemaKnownAtCompileTime() {
        return false;
    }

}
