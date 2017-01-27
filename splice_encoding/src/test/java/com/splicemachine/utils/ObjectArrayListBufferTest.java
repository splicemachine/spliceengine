/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

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
