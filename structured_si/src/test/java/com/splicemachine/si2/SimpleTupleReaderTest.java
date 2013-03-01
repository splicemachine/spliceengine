package com.splicemachine.si2;

import com.splicemachine.si2.relations.api.TupleHandler;
import com.splicemachine.si2.relations.simple.SimpleCell;
import com.splicemachine.si2.relations.simple.SimpleTuple;
import com.splicemachine.si2.relations.simple.SimpleTupleHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SimpleTupleReaderTest {

	@Test
	public void testSingleVal() throws Exception {
		List<SimpleCell> values = Arrays.asList(new SimpleCell("foo", "age", 5L, 21));
		SimpleTuple tuple = new SimpleTuple("fred", values);
		TupleHandler reader = new SimpleTupleHandler();
		Assert.assertEquals("key is wrong", "fred", reader.getKey(tuple));
		List result = reader.getCellsForColumn(tuple, "foo", "age");
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(21, reader.getCellValue(result.get(0)));
	}

	@Test
	public void testMultiValuesForOneColumn() throws Exception {
		List<SimpleCell> values = Arrays.asList(
				new SimpleCell("foo", "age", 5L, 21),
				new SimpleCell("foo", "age", 3L, 11),
				new SimpleCell("foo", "age", 11L, 41),
				new SimpleCell("foo", "age", 8L, 31));
		SimpleTuple tuple = new SimpleTuple("fred", values);
		TupleHandler reader = new SimpleTupleHandler();
		Assert.assertEquals("key is wrong", "fred", reader.getKey(tuple));
		List result = reader.getCellsForColumn(tuple, "foo", "age");
		Assert.assertEquals(4, result.size());
		Assert.assertEquals(41, reader.getCellValue(result.get(0)));
		Assert.assertEquals(31, reader.getCellValue(result.get(1)));
		Assert.assertEquals(21, reader.getCellValue(result.get(2)));
		Assert.assertEquals(11, reader.getCellValue(result.get(3)));

		Object latestValue = reader.getLatestCellForColumn(tuple, "foo", "age");
		Assert.assertEquals("foo", reader.getCellFamily(latestValue));
		Assert.assertEquals("age", reader.getCellQualifier(latestValue));
		Assert.assertEquals(41, reader.getCellValue(latestValue));
		Assert.assertEquals(11, reader.getCellTimestamp(latestValue));
	}

	@Test
	public void testMultiValuesManyColumns() throws Exception {
		List<SimpleCell> values = Arrays.asList(
				new SimpleCell("foo", "age", 5L, 21),
				new SimpleCell("foo", "age", 3L, 11),
				new SimpleCell("foo", "job", 11L, "baker"),
				new SimpleCell("foo", "alias", 8L, "joey"));
		SimpleTuple tuple = new SimpleTuple("fred", values);
		TupleHandler reader = new SimpleTupleHandler();

		List keyValues = reader.getCells(tuple);
		List<String> results = new ArrayList<String>();
		for (Object kv : keyValues) {
			results.add(kv.toString());
		}
		Assert.assertArrayEquals(new Object[]{
				new SimpleCell("foo", "age", 5L, 21).toString(),
				new SimpleCell("foo", "age", 3L, 11).toString(),
				new SimpleCell("foo", "job", 11L, "baker").toString(),
				new SimpleCell("foo", "alias", 8L, "joey").toString()},
				results.toArray());
	}
}
