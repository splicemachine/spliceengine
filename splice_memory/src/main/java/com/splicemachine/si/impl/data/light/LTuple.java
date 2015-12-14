package com.splicemachine.si.impl.data.light;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.*;

public class LTuple extends LOperationWithAttributes{
    final byte[] key;
    final List<KeyValue> values;
    final Integer lock;

    public LTuple(byte[] key, List<KeyValue> values) {
				this(key,values,Maps.<String,byte[]>newHashMap(),null);
    }

    public LTuple(byte[] key, List<KeyValue> values, Map<String, byte[]> attributes) {
				this(key,values,attributes,null);
    }

		public LTuple(byte[] key, List<KeyValue> values, Map<String, byte[]> attributes, Integer lock) {
				this.key = key;
				this.values = values;
				this.lock = lock;
				for(Map.Entry<String,byte[]> attributePair:attributes.entrySet()){
						super.setAttribute(attributePair.getKey(),attributePair.getValue());
				}
		}

    public LTuple(byte[] key, List<KeyValue> values, Integer lock) {
				this(key,values,Maps.<String,byte[]>newHashMap(),lock);
    }

		public List<KeyValue> getValues(){
				List<KeyValue> keyValues = Lists.newArrayList(values);
				LStore.sortValues(keyValues);
				return keyValues;
		}

		@Override
    public String toString() {
        return "<" + Bytes.toString(key) + " " + values + ">";
    }

}
