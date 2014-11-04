package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author Scott Fines
 * Date: 4/18/14
 */
public class ScanBuilder {
		private Scan scan;

		private int[] keyDecodingMap;
		private boolean[] keySortOrder;
		private int[] keyColumnEncodingOrder;

		private int[] columnTypes;
		private DescriptorSerializer[] serializers;

		private EntryPredicateFilter predicateFilter;

		private ObjectArrayList<Predicate> predicates;
		private FormatableBitSet scanColumnList;


		ScanBuilder startKey(DataValueDescriptor[] startKey,int startSearchOperator){
			return null;
		}

		ScanBuilder stopKey(DataValueDescriptor[] stopKey,int stopSearchOperator){
				return null;

		}

		ScanBuilder qualifiers(Qualifier[][] qualifiers){
				return null;

		}

}
