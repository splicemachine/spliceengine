package com.splicemachine.pipeline.constraint;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

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