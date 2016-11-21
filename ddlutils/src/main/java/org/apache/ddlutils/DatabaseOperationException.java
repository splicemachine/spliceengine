/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils;

/**
 * This exception is thrown when a database operation failed.
 *
 * @version $Revision: 289996 $
 */
public class DatabaseOperationException extends DdlUtilsException {
    /**
     * Constant for serializing instances of this class.
     */
    private static final long serialVersionUID = -3090677744278358036L;

    /**
     * Creates a new empty exception object.
     */
    public DatabaseOperationException() {
        super();
    }

    /**
     * Creates a new exception object.
     *
     * @param msg The exception message
     */
    public DatabaseOperationException(String msg) {
        super(msg);
    }

    /**
     * Creates a new exception object.
     *
     * @param baseEx The base exception
     */
    public DatabaseOperationException(Throwable baseEx) {
        super(baseEx);
    }

    /**
     * Creates a new exception object.
     *
     * @param msg    The exception message
     * @param baseEx The base exception
     */
    public DatabaseOperationException(String msg, Throwable baseEx) {
        super(msg, baseEx);
    }

}
