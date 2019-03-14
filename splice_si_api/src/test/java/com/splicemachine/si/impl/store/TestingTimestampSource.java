/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.si.impl.store;

import com.splicemachine.timestamp.api.TimestampSource;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class TestingTimestampSource implements TimestampSource{
    private long id = 0;
    private long memory = 0;

    @Override
    public long nextTimestamp() {
        synchronized (this) {
            id = id + 0x100;
            return id;
        }
    }

    @Override
    public void rememberTimestamp(long timestamp) {
        memory = timestamp;
    }

    @Override
    public long retrieveTimestamp() {
        return memory;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void refresh() {

    }
}
