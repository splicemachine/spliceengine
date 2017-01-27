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
