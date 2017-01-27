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