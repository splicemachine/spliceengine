package com.splicemachine.pipeline.writehandler;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.utils.SpliceLogUtils;
/**
 *
 * Attempt to share the call buffer between contexts using the context to lookup pre flush values...
 *
 *
 */
public class IndexSharedPreFlushHook implements PreFlushHook {
		static final Logger LOG = Logger.getLogger(IndexSharedPreFlushHook.class);
		private List<Pair<WriteContext,ObjectObjectOpenHashMap<KVPair,KVPair>>> sharedMainMutationList;

		public IndexSharedPreFlushHook() {
				sharedMainMutationList = new ArrayList<>();
		}

		public void registerContext(WriteContext context, ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap) {
				sharedMainMutationList.add(Pair.newPair(context, indexToMainMutationMap));
		}

		@Override
		public ObjectArrayList<KVPair> transform(ObjectArrayList<KVPair> buffer) throws Exception {
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "transform buffer rows=%d",buffer.size());
				ObjectArrayList<KVPair> newList = ObjectArrayList.newInstance();
				Object[] array = buffer.buffer;
				int size = buffer.size();
				for (int i = 0; i< size; i++) {
						KVPair indexPair = (KVPair) array[i];
						for (Pair<WriteContext,ObjectObjectOpenHashMap<KVPair,KVPair>> pair: sharedMainMutationList) {
								KVPair base = pair.getSecond().get(indexPair);
								if (base != null) {
										if (pair.getFirst().canRun(base))
												newList.add((KVPair)array[i]);
										break;
								}
						}
				}
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "transform returns buffer rows=%d",newList.size());
				return newList;
		}

		public void cleanup() {
				sharedMainMutationList.clear();
				sharedMainMutationList = null;
		}

}
