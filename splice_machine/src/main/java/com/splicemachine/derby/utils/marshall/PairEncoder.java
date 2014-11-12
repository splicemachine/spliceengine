package com.splicemachine.derby.utils.marshall;

import com.google.common.io.Closeables;
import com.splicemachine.hbase.KVPair;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class PairEncoder implements Closeable {
		protected final KeyEncoder keyEncoder;
		protected  final DataHash<ExecRow> rowEncoder;
		protected final KVPair.Type pairType;

		public PairEncoder(KeyEncoder keyEncoder, DataHash<ExecRow> rowEncoder, KVPair.Type pairType) {
				this.keyEncoder = keyEncoder;
				this.rowEncoder = rowEncoder;
				this.pairType = pairType;
		}

		public KVPair encode(ExecRow execRow) throws StandardException, IOException {
				byte[] key = keyEncoder.getKey(execRow);
				rowEncoder.setRow(execRow);
				byte[] row = rowEncoder.encode();

				return new KVPair(key,row,pairType);
		}

		public PairDecoder getDecoder(ExecRow template){
				return new PairDecoder(keyEncoder.getDecoder(),
								rowEncoder.getDecoder(), template);
		}

		@Override
		public void close() throws IOException {
				Closeables.closeQuietly(keyEncoder);
				Closeables.closeQuietly(rowEncoder);
		}
}
