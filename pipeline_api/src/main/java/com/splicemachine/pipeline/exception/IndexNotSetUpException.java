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
