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

package com.splicemachine.pipeline.exception;

import java.io.IOException;

/**
 * Thrown when the index code could not be setup right away, but a retry might succeed.
 *
 * @author Scott Fines
 * Created on: 3/22/13
 */
public class IndexNotSetUpException extends IOException {

    public IndexNotSetUpException() {
    }

    public IndexNotSetUpException(String message) {
        super(message);
    }

    public IndexNotSetUpException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexNotSetUpException(Throwable cause) {
        super(cause);
    }
}
