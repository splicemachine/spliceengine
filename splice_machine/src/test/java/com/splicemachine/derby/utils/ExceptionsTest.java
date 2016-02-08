package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.testsetup.PipelineTestDataEnv;
import com.splicemachine.pipeline.testsetup.PipelineTestEnvironment;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 5/21/13
 */
@Category(ArchitectureSpecific.class)
public class ExceptionsTest {
    private PipelineExceptionFactory pef;

    @Before
    public void setUp() throws Exception{
        PipelineTestDataEnv testEnv =PipelineTestEnvironment.loadTestDataEnvironment();
        pef = testEnv.pipelineExceptionFactory();
    }

    @Test
    public void testParseStandardException() throws Exception {
        StandardException se = StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
        se.printStackTrace();
        System.out.println("Converting to IOException");
        IOException spliceDoNotRetryIOException = Exceptions.getIOException(se,pef);
        spliceDoNotRetryIOException.printStackTrace();

        System.out.println("Converting back to StandardException");
        //check that it can be turned back into a StandardException
        StandardException converted = Exceptions.parseException(spliceDoNotRetryIOException,pef,null);
        Assert.assertEquals("Error codes incorrect!",se.getErrorCode(),converted.getErrorCode());
        Assert.assertEquals("Message incorrect!",se.getMessage(),converted.getMessage());
    }
}
