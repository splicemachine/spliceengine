/*

   Derby - Class com.splicemachine.db.impl.db.DatabaseContextImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.db;

import com.splicemachine.db.iapi.services.context.ContextImpl;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.db.Database;
import com.splicemachine.db.iapi.db.DatabaseContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.ExceptionSeverity;


/**
	A context that shutdowns down the database on a databsae exception.
*/
final class DatabaseContextImpl extends ContextImpl implements DatabaseContext
{

	private final Database	db;

	DatabaseContextImpl(ContextManager cm, Database db) {
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
        
        if (se.getSeverity() >= ExceptionSeverity.DATABASE_SEVERITY) {
            // DERBY-5108: Shut down the istat daemon thread before shutting
            // down the various modules belonging to the database. An active
            // istat daemon thread at the time of shutdown may result in
            // containers being reopened after the container cache has been
            // shut down. On certain platforms, this results in database
            // files that can't be deleted until the VM exits.
            DataDictionary dd = db.getDataDictionary();
            // dd is null if the db is an active slave db (replication)
            if (dd != null) {
                dd.disableIndexStatsRefresher();
            }
        }

        if (se.getSeverity() == ExceptionSeverity.DATABASE_SEVERITY) {
		    ContextService.getFactory().notifyAllActiveThreads(this);
            // This may be called multiple times, but is short-circuited
            // in the monitor.
		    Monitor.getMonitor().shutdown(db);
        }
	}

	public boolean equals(Object other) {
		if (other instanceof DatabaseContext) {
			return ((DatabaseContextImpl) other).db == db;
		}
		return false;
	}

	public int hashCode() {
		return db.hashCode();
	}

	public Database getDatabase() {return db;}
}
