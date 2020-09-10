/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.impl.hlc;

import com.splicemachine.timestamp.api.TimestampSource;

/**
 * Created by jleach on 4/21/16.
 */
public class HLCTimestampSource implements TimestampSource {
    public static final HLC hlc = new HLC();

    public HLCTimestampSource() {
    }


    @Override
    public long currentTimestamp() {
        return hlc.sendOrLocalEvent();
    }

    @Override
    public long nextTimestamp() {
        return hlc.sendOrLocalEvent();
    }

    @Override
    public void rememberTimestamp(long timestamp) {
        // No Op
    }

    @Override
    public long retrieveTimestamp() {
        return 0;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void bumpTimestamp(long timestamp) {

    }
}
