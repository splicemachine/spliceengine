package com.splicemachine.derby.utils;

import junit.framework.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created: 2/1/13 10:03 AM
 */
public class StringUtilsTest {
	@Test
	public void testStrip() throws Exception {
		String escape = "\"";
		String value = "\"10000\"";
		String result = StringUtils.strip(value,escape,'\\');
		Assert.assertEquals("10000",result);
	}

	@Test
	public void testStripEscaped() throws Exception{
		String escape ="\"";
		String value = "\\\"10000\"";
		String result = StringUtils.strip(value,escape,'\\');

		Assert.assertEquals("\"10000",result);
	}

    @Test
    public void testControlCharacters() throws Exception{
        String test = "\\t";
        String result = StringUtils.parseControlCharacters(test);
        Assert.assertEquals("\t",result);
    }

    @Test
    public void testControlCharactersWithLongString() throws Exception{
        String test = "hello goodbye\\t";
        String result = StringUtils.parseControlCharacters(test);
        Assert.assertEquals("hello goodbye\t",result);
    }

    @Test
    public void testControlCharactersEndingWithLongString() throws Exception{
        String test = "\\thello goodbye";
        String result = StringUtils.parseControlCharacters(test);
        Assert.assertEquals("\thello goodbye",result);
    }

    @Test
    public void testUnicodeControl() throws Exception{
        String test = "\\u0009";
        String result = StringUtils.parseControlCharacters(test);
        Assert.assertEquals("\t",result);
    }

    @Test
    public void testEndsWithUnicodeControl() throws Exception{
        String test = "hello goodbye\\u0009";
        String result = StringUtils.parseControlCharacters(test);
        Assert.assertEquals("hello goodbye\t",result);
    }

    @Test
    public void testStartsWithUnicodeControl() throws Exception{
        String test = "\\u0009hello goodbye";
        String result = StringUtils.parseControlCharacters(test);
        Assert.assertEquals("\thello goodbye",result);
    }
}
