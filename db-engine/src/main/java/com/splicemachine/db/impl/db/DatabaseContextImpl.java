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

package com.splicemachine.db.impl.db;

import com.splicemachine.db.iapi.db.InternalDatabase;
import com.splicemachine.db.iapi.services.context.ContextImpl;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.db.DatabaseContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.ExceptionSeverity;


/**
    A context that shutdowns down the database on a databsae exception.
*/
final class DatabaseContextImpl extends ContextImpl implements DatabaseContext
{

    private final InternalDatabase db;

    DatabaseContextImpl(ContextManager cm, InternalDatabase db) {
        super(cm, DatabaseContextImpl.CONTEXT_ID);
        this.db = db;
    }

    public void cleanupOnError(Throwable t) {
        if (!(t instanceof StandardException)) return;
        StandardException se = (StandardException)t;

        // Ensure the context is popped if the session is
        // going away.
        if (se.getSeverity() < ExceptionSeverity.SESSION_SEVERITY)
            return;

        popMe();
        
        if (se.getSeverity() == ExceptionSeverity.DATABASE_SEVERITY) {
            ContextService.getFactory().notifyAllActiveThreads(this);
            // This may be called multiple times, but is short-circuited
            // in the monitor.
            Monitor.getMonitor().shutdown(db);
        }
    }

    public boolean equals(Object other) {
        return other instanceof DatabaseContext && ((DatabaseContextImpl) other).db == db;
    }

    public int hashCode() {
        return db.hashCode();
    }

    public InternalDatabase getDatabase() {return db;}
}
