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
        IOException spliceDoNotRetryIOException = Exceptions.getIOException(se,pef);

        //check that it can be turned back into a StandardException
        StandardException converted = Exceptions.parseException(spliceDoNotRetryIOException,pef,null);
        Assert.assertEquals("Error codes incorrect!",se.getErrorCode(),converted.getErrorCode());
        Assert.assertEquals("Message incorrect!",se.getMessage(),converted.getMessage());
    }
}
