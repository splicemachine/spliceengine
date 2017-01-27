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

package com.splicemachine.db.iapi.sql.conn;

import java.lang.String;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;

/**
 * An implementation of this interface encapsulates some of the SQL
 * session context's state variables, cf. SQL 2003, section 4.37.3,
 * notably those which we need to save and restore when entering a
 * stored procedure or function (which can contain SQL and thus a
 * nested connection), cf. 4.37.3, 4.27.3 and 4.34.1.1.  <p> Presently
 * this set contains the following properties: <ul> <li>current
 * role</li> <li>current schema</li> </ul>
 *
 * The standard specifies that the authorization stack be copied onto
 * the new SQL session context before it is pushed (and possibly
 * modifed) with a new cell for the authorization ids (user, role). In
 * our implementation we merge these two stacks for now. Also, the
 * authorization id of current user is not represented yet, since it
 * can not be modified in a session; Derby can not run routines with
 * definer's rights yet.
 * <p>
 * SQL session context is implemented as follows: Statements at root
 * connection level use the instance held by the the lcc, nested
 * connections maintain instances of SQLSessionContext, held by the
 * activation of the calling statement. This forms a logical stack as
 * required by the standard. The statement context also holds a
 * reference to the current SQLSessionContext.
 * <p>
 * When a dynamic result set references e.g. current role, the value
 * retrieved will always be that of the current role when the
 * statement is logically executed (inside procedure/function), not
 * the current value when the result set is accessed outside the
 * stored procedure/function.  This works since the nested SQL session
 * context is kept by the caller activation, so even though the
 * statement context of the call has been popped, we can get at the
 * final state of the nested SQL session context since the caller's
 * activation is alive as long as dynamic result sets need it).
 * <p>
 * If more than one nested connection is used inside a shared
 * procedure, they will share the same nested SQL session
 * context. Since the same dynamic call context is involved, this
 * seems correct.
 *
 * @see com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext#setupNestedSessionContext
 */

public interface SQLSessionContext {

    /**
     * Set the SQL role of this SQL connection context
     */
    public void setRole(String role);

    /**
     * Get the SQL role of this SQL connection context
     */
    public String getRole();

    /**
     * Set the SQL current user of this SQL connection context
     */
    public void setUser(String user);

    /**
     * Get the SQL current user of this SQL connection context
     */
    public String getCurrentUser();

    /**
     * Set the schema of this SQL connection context
     */
    public void setDefaultSchema(SchemaDescriptor sd);

    /**
     * Get the schema of this SQL connection context
     */
    public SchemaDescriptor getDefaultSchema();
}
