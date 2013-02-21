package com.splicemachine.derby.impl.load;

import org.junit.Assert;
import org.junit.Test;

public class SpliceImportCoprocessorTest {
	@Test
	public void testParseCsvLine() throws Exception {

		// arbitrary character as delimiter with trailing delimiter
		Assert.assertArrayEquals(new String[]{"a", "b", ""},
				SpliceImportCoprocessor.parseCsvLine("x", "axbx"));

		// delimiter in the data
		Assert.assertArrayEquals(new String[]{"joe", "billy, the kid", "jim"},
				SpliceImportCoprocessor.parseCsvLine(",", "\"joe\",\"billy, the kid\",jim"));

		// empty field in the middle
		Assert.assertArrayEquals(new String[]{"joe", "", "jim"},
				SpliceImportCoprocessor.parseCsvLine(",", "joe,,jim"));

		// tabs as delimiters
		Assert.assertArrayEquals(new String[]{"joe", "jim"},
				SpliceImportCoprocessor.parseCsvLine("\t", "joe\tjim"));
	}

	@Test(expected=IndexOutOfBoundsException.class)
	public void testParseCsvLineNoDelimiterSpecified() throws Exception {
		SpliceImportCoprocessor.parseCsvLine("", "joe\tjim");
	}

	@Test(expected=NullPointerException.class)
	public void testParseCsvLineNullDelimiterSpecified() throws Exception {
		SpliceImportCoprocessor.parseCsvLine(null, "joe\tjim");
	}
}
