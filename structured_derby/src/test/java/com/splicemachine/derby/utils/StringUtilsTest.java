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
}
