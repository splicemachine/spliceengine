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

package com.splicemachine.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.spark_project.guava.base.Predicate;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.collect.Lists;

public class ObjectArrayListBufferTest {
	protected static Map<String,String> map = new HashMap<String,String>();
	protected static ObjectObjectOpenHashMap<String,String> backingMap = new ObjectObjectOpenHashMap<String,String>();

	static {
		map.put("John", "Leach");
		map.put("Jenny", "Leach");
		map.put("Molly", "Leach");
		map.put("Katie", "Leach");
		backingMap.put("John", "Leach");
		backingMap.put("Jenny", "Leach");
		backingMap.put("Molly", "Leach");
		backingMap.put("Katie", "Leach");
		
	}
	
	
	@Test
	public void listBufferFilterTest() {
		List<String> names = Lists.newArrayList("John","Jenny","Molly");
		
	     List<String> newList = Lists.newArrayList(Collections2.filter(names,new Predicate<String>() {
			@Override
			public boolean apply(String input) {
				if (map.containsKey(input))
					return true;
				return false;
			}
         }));
		Assert.assertEquals(3, newList.size());
		
	}
	
	@Test
	public void objectArrayListBufferTest() {
		ObjectArrayList<String> names = ObjectArrayList.from("John","Jenny","Molly");
		ObjectArrayList<String> newList = ObjectArrayList.newInstance();
		Object[] buffer = names.buffer;
		int size = names.size();
		for (int i =0; i<size; i++) {
			if (backingMap.containsKey((String)buffer[i]))
				newList.add((String)buffer[i]);
		}
		Assert.assertEquals(3, newList.size());
	}
	
}
