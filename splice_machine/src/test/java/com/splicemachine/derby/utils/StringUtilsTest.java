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

import com.splicemachine.db.impl.ast.StringUtils;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Scott Fines
 *         Created: 2/1/13 10:03 AM
 */
@Category(ArchitectureIndependent.class)
public class StringUtilsTest {
	@Test
	public void testStrip() throws Exception {
		String escape = "\"";
		String value = "\"10000\"";
		String result = StringUtils.strip(value, escape, '\\');
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
