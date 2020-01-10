/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.constraint;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;

@Category(ArchitectureIndependent.class)
public class ConstraintContextTest {

    @Test
    public void withInsertedMessage() {
        // given
        ConstraintContext context1 = new ConstraintContext("aa", "bb", "cc");
        // when
        ConstraintContext context2 = context1.withInsertedMessage(0, "ZZ");
        ConstraintContext context3 = context1.withInsertedMessage(1, "ZZ");
        ConstraintContext context4 = context1.withInsertedMessage(2, "ZZ");
        ConstraintContext context5 = context1.withInsertedMessage(3, "ZZ");
        // then
        assertArrayEquals(new String[]{"ZZ", "aa", "bb", "cc"}, context2.getMessages());
        assertArrayEquals(new String[]{"aa", "ZZ", "bb", "cc"}, context3.getMessages());
        assertArrayEquals(new String[]{"aa", "bb", "ZZ", "cc"}, context4.getMessages());
        assertArrayEquals(new String[]{"aa", "bb", "cc", "ZZ"}, context5.getMessages());
    }

    @Test
    public void withOutMessage() {
        // given
        ConstraintContext context1 = new ConstraintContext("aa", "bb", "cc");
        // when
        ConstraintContext context2 = context1.withoutMessage(0);
        ConstraintContext context3 = context1.withoutMessage(1);
        ConstraintContext context4 = context1.withoutMessage(2);
        // then
        assertArrayEquals(new String[]{"bb", "cc"}, context2.getMessages());
        assertArrayEquals(new String[]{"aa", "cc"}, context3.getMessages());
        assertArrayEquals(new String[]{"aa", "bb"}, context4.getMessages());
    }


    @Test
    public void withMessage() {
        // given
        ConstraintContext context1 = new ConstraintContext("aa", "bb", "cc");
        // when
        ConstraintContext context2 = context1.withMessage(0, "ZZ");
        ConstraintContext context3 = context1.withMessage(1, "ZZ");
        ConstraintContext context4 = context1.withMessage(2, "ZZ");
        // then
        assertArrayEquals(new String[]{"ZZ", "bb", "cc"}, context2.getMessages());
        assertArrayEquals(new String[]{"aa", "ZZ", "cc"}, context3.getMessages());
        assertArrayEquals(new String[]{"aa", "bb", "ZZ"}, context4.getMessages());
    }
}
