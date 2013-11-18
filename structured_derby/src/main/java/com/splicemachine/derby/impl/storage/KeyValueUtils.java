package com.splicemachine.derby.impl.storage;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import org.apache.hadoop.hbase.KeyValue;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class KeyValueUtils {

		private KeyValueUtils(){}

		public static KeyValue matchKeyValue(Iterable<KeyValue> kvs,byte[] columnFamily, byte[] qualifier){
				for(KeyValue kv:kvs){
						if(kv.matchingColumn(columnFamily,qualifier))
								return kv;
				}
				return null;
		}

		public static KeyValue matchKeyValue(KeyValue[] kvs,byte[] columnFamily, byte[] qualifier){
				for(KeyValue kv:kvs){
						if(kv.matchingColumn(columnFamily,qualifier))
								return kv;
				}
				return null;
		}

		public static KeyValue matchDataColumn(KeyValue[] kvs){
				return matchKeyValue(kvs, SpliceConstants.DEFAULT_FAMILY_BYTES,
								RowMarshaller.PACKED_COLUMN_KEY);
		}

		public static KeyValue matchDataColumn(List<KeyValue> kvs){
				return matchKeyValue(kvs, SpliceConstants.DEFAULT_FAMILY_BYTES,
								RowMarshaller.PACKED_COLUMN_KEY);
		}
}
