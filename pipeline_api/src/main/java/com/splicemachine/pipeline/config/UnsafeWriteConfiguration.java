/*
 *
 *  * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *  *
 *  * This file is part of Splice Machine.
 *  * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 *  * GNU Affero General Public License as published by the Free Software Foundation, either
 *  * version 3, or (at your option) any later version.
 *  * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  * See the GNU Affero General Public License for more details.
 *  * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 *  * If not, see <http://www.gnu.org/licenses/>.
 *
 *
 */

package com.splicemachine.pipeline.config;

import org.apache.log4j.Logger;

/**
 *
 */
public class UnsafeWriteConfiguration extends ForwardingWriteConfiguration {
    private static final Logger LOG = Logger.getLogger(UnsafeWriteConfiguration.class);
    protected boolean skipWAL;
    protected boolean skipConflictDetection;

    public UnsafeWriteConfiguration(WriteConfiguration delegate,
                                    boolean skipConflictDetection, boolean skipWAL) {
        super(delegate);
        this.skipWAL = skipWAL;
        this.skipConflictDetection = skipConflictDetection;
    }

    @Override
    public String toString() {
        return String.format("UnsafeWriteConfiguration{delegate=%s}",delegate);
    }

    @Override
    public boolean skipConflictDetection() {
        return skipConflictDetection;
    }

    @Override
    public boolean skipWAL() {
        return skipWAL;
    }
}

