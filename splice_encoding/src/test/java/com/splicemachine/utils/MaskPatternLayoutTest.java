package com.splicemachine.utils;

import com.splicemachine.utils.logging.MaskPatternLayout;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class MaskPatternLayoutTest {

    private static final String maskString = "MASK STRING";

    @Test
    public void testMatchPattern() {
        Pattern maskPattern = Pattern.compile("abc ([0-9]+)");
        String maskedMessage = MaskPatternLayout.maskMessage("abc 1234 efg", maskPattern, maskString);
        Assert.assertEquals("abc " + maskString+ " efg", maskedMessage);
    }

    @Test
    public void testMatchPatternMultipleTimes() {
        Pattern maskPattern = Pattern.compile("([0-9]+)");
        String maskedMessage = MaskPatternLayout.maskMessage("1234 1234", maskPattern, maskString);
        Assert.assertEquals(maskString + " " + maskString, maskedMessage);
    }

    @Test
    public void testMatchPatternStart() {
        Pattern maskPattern = Pattern.compile("([0-9]+)");
        String maskedMessage = MaskPatternLayout.maskMessage("1234 efg", maskPattern, maskString);
        Assert.assertEquals(maskString+ " efg", maskedMessage);
    }

    @Test
    public void testMatchPatternEnd() {
        Pattern maskPattern = Pattern.compile("abc ([0-9]+)");
        String maskedMessage = MaskPatternLayout.maskMessage("abc 1234", maskPattern, maskString);
        Assert.assertEquals("abc " + maskString, maskedMessage);
    }

    @Test
    public void testMultiMatchPattern() {
        Pattern maskPattern = Pattern.compile("([0-9]+)(?:[A-Z]+)([a-z]+)");
        String maskedMessage = MaskPatternLayout.maskMessage("abc 1234ABCefg", maskPattern,
                maskString);
        Assert.assertEquals("abc " + maskString + "ABC" + maskString, maskedMessage);
    }

    @Test
    public void testMultiMatchPatternContinue() {
        Pattern maskPattern = Pattern.compile("([0-9]+)([a-z]+)");
        String maskedMessage = MaskPatternLayout.maskMessage("abc 1234efg", maskPattern,
                maskString);
        Assert.assertEquals("abc " + maskString + maskString, maskedMessage);
    }

    @Test
    public void testNoMatchPattern() {
        Pattern maskPattern = Pattern.compile("([0-9]+)");
        String maskedMessage = MaskPatternLayout.maskMessage("abc efg", maskPattern, maskString);
        Assert.assertEquals("abc efg", maskedMessage);
    }
}
