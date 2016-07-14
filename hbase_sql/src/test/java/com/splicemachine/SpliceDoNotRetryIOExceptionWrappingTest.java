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

import com.splicemachine.SpliceDoNotRetryIOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SpliceDoNotRetryIOExceptionWrappingTest {

    @Test
    public void testUnwrap() throws Exception {

        // given
        StandardException stdExceptionIn = StandardException.newException(SQLState.LANG_COLUMN_ID, "arg1", "arg2", "arg3", "arg4");

        // when
        SpliceDoNotRetryIOException wrapped = SpliceDoNotRetryIOExceptionWrapping.wrap(stdExceptionIn);
        StandardException stdExceptionOut = (StandardException) SpliceDoNotRetryIOExceptionWrapping.unwrap(wrapped);

        // then
        assertEquals("Column Id", stdExceptionOut.getMessage());
        assertArrayEquals(new String[]{"arg1", "arg2", "arg3", "arg4"}, stdExceptionOut.getArguments());
        assertEquals("42Z42", stdExceptionOut.getSqlState());

    }

}