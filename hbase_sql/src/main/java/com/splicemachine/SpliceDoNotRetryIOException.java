/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
