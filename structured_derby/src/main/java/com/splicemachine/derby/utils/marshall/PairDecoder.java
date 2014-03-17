package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.Cell;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.hbase.KVPair;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class PairDecoder {
		private final KeyDecoder keyDecoder;
		private final KeyHashDecoder rowDecoder;
		private final ExecRow templateRow;

		public PairDecoder(KeyDecoder keyDecoder,
											 KeyHashDecoder rowDecoder,
											 ExecRow templateRow) {
				this.keyDecoder = keyDecoder;
				this.rowDecoder = rowDecoder;
				this.templateRow = templateRow;
		}

		public ExecRow decode(Cell data) throws StandardException{
				templateRow.resetRowArray();
				keyDecoder.decode(CellUtils.getBuffer(data),data.getRowOffset(),data.getRowLength(),templateRow);
				rowDecoder.set(CellUtils.getBuffer(data),data.getValueOffset(),data.getValueLength());
				rowDecoder.decode(templateRow);
				return templateRow;
		}

		public ExecRow decode(KVPair kvPair) throws StandardException{
				templateRow.resetRowArray();
				keyDecoder.decode(kvPair.getRow(),0,kvPair.getRow().length,templateRow);
				rowDecoder.set(kvPair.getValue(),0,kvPair.getValue().length);
				rowDecoder.decode(templateRow);
				return templateRow;
		}

		/*
		 *
		 *  < a | b |c >
		 *    1 | 2 | 3
		 *
		 *  sort (a) -->
		 *  Row Key: 1
		 *  Row Data: 2 | 3
		 *
		 *  group (a,b) ->
		 *  Row Key: a | b
		 *  Row Data: aggregate(c)
		 */

		public int getKeyPrefixOffset(){
				return keyDecoder.getPrefixOffset();
		}

		public ExecRow getTemplate() {
				return templateRow;
		}

		@Override
		public String toString() {
			return String.format("PairDecoder { keyDecoder=%s rowDecoder=%s, templateRow=%s}",keyDecoder,rowDecoder,templateRow);
		}		
}
