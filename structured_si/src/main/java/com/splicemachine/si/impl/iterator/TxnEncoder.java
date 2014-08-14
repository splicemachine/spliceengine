package com.splicemachine.si.impl.iterator;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public class TxnEncoder {

		public static byte[] toBytes(Txn txn){
				MultiFieldEncoder encoder = MultiFieldEncoder.create(9);
				encoder.encodeNext(txn.getBeginTimestamp());
				TxnView parentTxn = txn.getParentTxnView();
				if(parentTxn!=null)
						encoder.encodeNext(parentTxn.getTxnId());
				else
						encoder.encodeEmpty();

				encoder.encodeNext(txn.isDependent())
								.encodeNext(txn.allowsWrites())
								.encodeNext(txn.isAdditive())
								.encodeNext(txn.getIsolationLevel().getLevel())
								.encodeNext(txn.getCommitTimestamp());
				long effectiveCommitTimestamp = txn.getEffectiveCommitTimestamp();
				if(effectiveCommitTimestamp>0)
						encoder.encodeNext(effectiveCommitTimestamp);
				else
						encoder.encodeEmpty();
				if(!txn.isDependent())
						encoder.encodeNext(txn.getCommitTimestamp()); //global commit timestamp
				else
						encoder.encodeEmpty();

				return encoder.build();
		}

		public Txn fromBytes(byte[] data){
				MultiFieldDecoder decoder = MultiFieldDecoder.wrap(data);

				return null;
		}
}
