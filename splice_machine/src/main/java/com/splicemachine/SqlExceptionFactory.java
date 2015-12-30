package com.splicemachine;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.ExceptionFactory;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public interface SqlExceptionFactory extends ExceptionFactory{
    IOException asIOException(StandardException se);
}
