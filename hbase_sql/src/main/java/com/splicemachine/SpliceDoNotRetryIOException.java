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

package com.splicemachine;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.log4j.Logger;

/**
 * We use this to transfer StandardExceptions over the wire via hbase RPC as IOExceptions.
 *
 * @see SpliceDoNotRetryIOExceptionWrapping
 */
public class SpliceDoNotRetryIOException extends DoNotRetryIOException {

    private static final long serialVersionUID = -733330421228198468L;
    private static final Logger LOG = Logger.getLogger(SpliceDoNotRetryIOException.class);

    public SpliceDoNotRetryIOException() {
        super();
        SpliceLogUtils.trace(LOG, "instance");
    }

    public SpliceDoNotRetryIOException(String message, Throwable cause) {
        super(message, cause);
        SpliceLogUtils.trace(LOG, "instance with message %s and throwable %s", message, cause);
    }

    public SpliceDoNotRetryIOException(String message) {
        super(message);
        SpliceLogUtils.trace(LOG, "instance with message %s", message);
    }

    @Override
    public String getMessage() {
        LOG.trace("getMessage");
        return super.getMessage();
    }
}
