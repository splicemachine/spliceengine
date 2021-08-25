/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
 *
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
package com.splicemachine.db.iapi.sql.dictionary;

import org.joda.time.DateTime;

public class SnapshotDescriptor extends TupleDescriptor  {
    private long snapshotId;
    private String scope;
    private String scopeName;
    private String status;
    private DateTime beginTimestamp;
    private DateTime endTimestamp;


    public SnapshotDescriptor(long snapshotId,
                              String scope,
                              String scopeName,
                              String status,
                              DateTime beginTimestamp,
                              DateTime endTimestamp) {
        this.snapshotId = snapshotId;
        this.scope = scope;
        this.scopeName = scopeName;
        this.status = status;
        this.beginTimestamp = beginTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public String getScope() {
        return scope;
    }

    public String getScopeName() {
        return scopeName;
    }

    public String getStatus() {
        return status;
    }

    public DateTime getBeginTimestamp() {
        return beginTimestamp;
    }

    public DateTime getEndTimestamp() {
        return endTimestamp;
    }

    public long getSnapshotId() {
        return snapshotId;
    }
}
