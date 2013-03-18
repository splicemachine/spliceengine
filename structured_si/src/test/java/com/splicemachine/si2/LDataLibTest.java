package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.light.LDataLib;
import com.splicemachine.si2.data.light.LKeyValue;
import com.splicemachine.si2.data.light.LTuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LDataLibTest {

	@Test
	public void testSingleVal() throws Exception {
		List<LKeyValue> values = Arrays.asList(new LKeyValue("fred", "foo", "age", 5L, 21));
		LTuple tuple = new LTuple("fred", values);
		SDataLib reader = new LDataLib();
		Assert.assertEquals("key is wrong", "fred", reader.getResultKey(tuple));
		List result = reader.getResultColumn(tuple, "foo", "age");
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(21, reader.getKeyValueValue(result.get(0)));
	}

	@Test
	public void testMultiValuesForOneColumn() throws Exception {
		List<LKeyValue> values = Arrays.asList(
				new LKeyValue("fred", "foo", "age", 5L, 21),
				new LKeyValue("fred", "foo", "age", 3L, 11),
				new LKeyValue("fred", "foo", "age", 11L, 41),
				new LKeyValue("fred", "foo", "age", 8L, 31));
		LTuple tuple = new LTuple("fred", values);
		SDataLib reader = new LDataLib();
		Assert.assertEquals("key is wrong", "fred", reader.getResultKey(tuple));
		List result = reader.getResultColumn(tuple, "foo", "age");
		Assert.assertEquals(4, result.size());
		Assert.assertEquals(41, reader.getKeyValueValue(result.get(0)));
		Assert.assertEquals(31, reader.getKeyValueValue(result.get(1)));
		Assert.assertEquals(21, reader.getKeyValueValue(result.get(2)));
		Assert.assertEquals(11, reader.getKeyValueValue(result.get(3)));

		Object latestValue = reader.getResultValue(tuple, "foo", "age");
		Assert.assertEquals(41, latestValue);
	}

	@Test
	public void testMultiValuesManyColumns() throws Exception {
		List<LKeyValue> values = Arrays.asList(
				new LKeyValue("fred", "foo", "age", 5L, 21),
				new LKeyValue("fred", "foo", "age", 3L, 11),
				new LKeyValue("fred", "foo", "job", 11L, "baker"),
				new LKeyValue("fred", "foo", "alias", 8L, "joey"));
		LTuple tuple = new LTuple("fred", values);
		SDataLib reader = new LDataLib();

		List keyValues = reader.listResult(tuple);
		List<String> results = new ArrayList<String>();
		for (Object kv : keyValues) {
			results.add(kv.toString());
		}
		Assert.assertArrayEquals(new Object[]{
				new LKeyValue("fred", "foo", "age", 5L, 21).toString(),
				new LKeyValue("fred", "foo", "age", 3L, 11).toString(),
				new LKeyValue("fred", "foo", "alias", 8L, "joey").toString(),
                new LKeyValue("fred", "foo", "job", 11L, "baker").toString()},
				results.toArray());
	}
}
