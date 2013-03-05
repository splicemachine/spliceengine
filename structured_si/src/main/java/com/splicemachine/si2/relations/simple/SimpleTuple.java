package com.splicemachine.si2.relations.simple;

import com.splicemachine.si2.relations.api.TuplePut;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleTuple implements TuplePut {
	final String key;
	final List<SimpleCell> values;
	final Map<String,Object> attributes;
    final SimpleLock lock;

	public SimpleTuple(String key, List<SimpleCell> values) {
		this.key = key;
		this.values = values;
        this.attributes = new HashMap<String, Object>();
        this.lock = null;
	}

	public SimpleTuple(String key, List<SimpleCell> values, Map<String,Object> attributes) {
		this.key = key;
		this.values = values;
		this.attributes = attributes;
        this.lock = null;
	}

    public SimpleTuple(String key, List<SimpleCell> values, SimpleLock lock) {
        this.key = key;
        this.values = values;
        this.attributes = new HashMap<String, Object>();
        this.lock = lock;
    }

    @Override
	public String toString() {
		return "<" + key + " " + values + ">";
	}
}
