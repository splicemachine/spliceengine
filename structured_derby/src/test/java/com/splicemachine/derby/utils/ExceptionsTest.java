package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 5/21/13
 */
public class ExceptionsTest {
    @Test
    public void testParseStandardException() throws Exception {
        StandardException se = StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
        se.printStackTrace();
        System.out.println("Converting to IOException");
        IOException spliceDoNotRetryIOException = Exceptions.getIOException(se);
        spliceDoNotRetryIOException.printStackTrace();

        System.out.println("Converting back to StandardException");
        //check that it can be turned back into a StandardException
        StandardException converted = Exceptions.parseException(spliceDoNotRetryIOException);
        Assert.assertEquals("Error codes incorrect!",se.getErrorCode(),converted.getErrorCode());
        Assert.assertEquals("Message incorrect!",se.getMessage(),converted.getMessage());
    }
}
