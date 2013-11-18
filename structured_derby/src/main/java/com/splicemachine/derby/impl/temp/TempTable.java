package com.splicemachine.derby.impl.temp;

import com.splicemachine.derby.utils.marshall.SpreadBucket;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Representation of the current state of a TempTable.
 *
 * Implementation note(-sf-): This was deliberately constructed
 * to NOT be a singleton, in case we ever decided to go with
 * multiple TempTables.
 *
 * @author Scott Fines
 * Date: 11/18/13
 */
public class TempTable {
		private final byte[] tempTableName;
		private AtomicReference<SpreadBucket> spread;

		public TempTable(byte[] tempTableName) {
				this.tempTableName = tempTableName;
				this.spread = new AtomicReference<SpreadBucket>(SpreadBucket.SIXTEEN);
		}

		public SpreadBucket getCurrentSpread() {
				return spread.get();
		}
		//TODO -sf- implement dynamic changes
}
