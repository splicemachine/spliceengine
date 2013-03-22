package com.splicemachine.derby.impl.sql.execute.index;

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
