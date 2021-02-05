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
