/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.pipeline.writer;

import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.client.BulkWrites;

import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Writer which regulates how many concurrent writes are allowed, gating flushes as necessary (forcing flushes
 * onto the calling thread if the maximum is exceeded.
 *
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public class RegulatedWriter implements Writer {
    private final Writer delegate;

    public RegulatedWriter(Writer delegate) {
        this.delegate = delegate;
    }

    @Override
    public Future<WriteStats> write(byte[] tableName,
                                    BulkWrites action,
                                    WriteConfiguration writeConfiguration) throws ExecutionException {
        return delegate.write(tableName, action, writeConfiguration);
    }

    @Override
    public void stopWrites() {
        throw new UnsupportedOperationException("Stop underlying writer instance instead");
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        throw new UnsupportedOperationException("register underlying writer instance instead");
    }

}