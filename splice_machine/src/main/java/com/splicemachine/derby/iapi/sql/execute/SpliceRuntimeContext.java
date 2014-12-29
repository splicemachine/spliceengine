package com.splicemachine.derby.iapi.sql.execute;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.metrics.*;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.utils.kryo.KryoPool;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SpliceRuntimeContext<Row> implements Externalizable,MetricFactory {
		private static final long serialVersionUID = 1l;
		private IntIntOpenHashMap paths = new IntIntOpenHashMap();
		/*
		 * Flag to determine whether or not the operation is being run as part of a Task process, or as part of a scan.
		 * This is only set to true during the task execution phase.
		 */
		private boolean isSink;
		private Row scanStartOverride;
		private byte[] currentTaskId;
      private byte[] parentTaskId;
		/*Only non-null on the node where the JDBC connection is held*/
		private transient StatementInfo statementInfo;
		private boolean firstStepInMultistep;
		/*
		 * Hash bucket to use for sink operations which do not spread data themselves.
		 *
		 * For example, the SortOperation can't spread data around multiple buckets, so
		 * it will use this hashBucket to determine which bucket to go to. The
		 * bucket itself will be generated randomly, to (hopefully) spread data from multiple
		 * concurrent operations across multiple buckets.
		 */
		private byte hashBucket;

		/*
		 * Flag to denote whether or not this task should record trace metrics or not.
		 * When set to true, the operation should record relevant information for its operation.
		 */
		private boolean recordTraceMetrics;
		private transient KryoPool kryoPool;
		private TempTable tempTable;
        private long statementId;

    /*
     * Useful for passing the transaction around
     */
    private transient TxnView txn;

    public SpliceRuntimeContext(){
        this(null);
    }


    public SpliceRuntimeContext(TxnView txn) {
				this(SpliceDriver.driver().getTempTable(), SpliceKryoRegistry.getInstance(),txn);
		}

    public SpliceRuntimeContext(TempTable tempTable,KryoPool kryoPool){
        this(tempTable, kryoPool,null);
    }

		public SpliceRuntimeContext(TempTable tempTable,KryoPool kryoPool,TxnView txn){
				this.tempTable = tempTable;
				this.hashBucket = tempTable.getCurrentSpread().bucket((int) System.currentTimeMillis());
				this.kryoPool = kryoPool;
        this.txn = txn;
		}

		public static SpliceRuntimeContext generateLeftRuntimeContext(TxnView txn,int resultSetNumber) {
				SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext(txn);
				spliceRuntimeContext.addPath(resultSetNumber, 0);
				return spliceRuntimeContext;
		}

		public static SpliceRuntimeContext generateRightRuntimeContext(TxnView txn,int resultSetNumber) {
				SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext(txn);
				spliceRuntimeContext.addPath(resultSetNumber, 1);
				return spliceRuntimeContext;
		}

		public static SpliceRuntimeContext generateSinkRuntimeContext(TxnView txn,boolean firstStepInMultistep) {
				SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext(txn);
				spliceRuntimeContext.firstStepInMultistep = firstStepInMultistep;
				return spliceRuntimeContext;
		}

    public TxnView getTxn() {
        return txn;
    }

    public void setTxn(TxnView txn) {
        this.txn = txn;
    }

		public void addLeftRuntimeContext(int resultSetNumber) {
				this.addPath(resultSetNumber, 0);
		}

		public void addRightRuntimeContext(int resultSetNumber) {
				this.addPath(resultSetNumber, 1);
		}

		public void addSinkRuntimeContext(boolean firstStepInMultistep) {
				this.firstStepInMultistep = firstStepInMultistep;
		}


		public SpliceRuntimeContext copy() {
				SpliceRuntimeContext copy = new SpliceRuntimeContext(txn);
				for (IntCursor path : paths.keys()) {
						copy.addPath(path.value, paths.get(path.value));
				}
				copy.scanStartOverride = scanStartOverride;
				copy.hashBucket = hashBucket;
				copy.isSink = isSink;
				copy.currentTaskId = currentTaskId;
				copy.statementInfo = statementInfo;
                copy.statementId = statementId;
				copy.recordTraceMetrics = recordTraceMetrics;
        copy.txn = txn;
				return copy;
		}


		public void setStatementInfo(StatementInfo statementInfo){
				this.statementInfo = statementInfo;
                this.statementId = statementInfo.getStatementUuid();
		}

    public void addPath(int resultSetNumber, int state) {
    	paths.put(resultSetNumber, state);
    }

    public void addPath(int resultSetNumber, Side side) {
    	paths.put(resultSetNumber, side.stateNum);
    }

    public Side getPathSide(int resultSetNumber) {
    	return Side.getSide(paths.get(resultSetNumber));
    }

    public boolean hasPathForResultset(int resultSetNumber) {
    	return paths.containsKey(resultSetNumber);
    }

    public boolean isLeft(int resultSetNumber) {
    	return paths.get(resultSetNumber) == Side.LEFT.stateNum;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(paths.size());
        for (IntCursor resultSet : paths.keys()) {
            out.writeInt(resultSet.value);
            out.writeInt(paths.get(resultSet.value));
        }
        out.writeByte(hashBucket);
        out.writeBoolean(firstStepInMultistep);
        out.writeBoolean(recordTraceMetrics);
        out.writeBoolean(statementInfo != null);
        if (statementInfo != null) {
            out.writeLong(statementInfo.getStatementUuid());
        }
        TransactionOperations.getOperationFactory().writeTxn(txn,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        paths = new IntIntOpenHashMap(size);
        for (int i = 0; i < size; i++) {
        	paths.put(in.readInt(), in.readInt());
        }
        hashBucket = in.readByte();
        firstStepInMultistep = in.readBoolean();
        this.recordTraceMetrics = in.readBoolean();
        if(in.readBoolean()) {
            this.statementId = in.readLong();
        }
        this.txn = TransactionOperations.getOperationFactory().readTxn(in);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SpliceRuntimeContext {  ");
        sb.append(", hashBucket=");
        sb.append(hashBucket);
        sb.append(", isSink=");
        sb.append(isSink);
        sb.append(" }");
        return sb.toString();
    }

		public void markAsSink() { isSink = true; }

    public void unMarkAsSink() { isSink = false; }

		public boolean isSink() { return isSink; }

		public void recordTraceMetrics(){ this.recordTraceMetrics = true; }

		@Override
		public Timer newTimer(){
				if(!recordTraceMetrics) return Metrics.noOpTimer();
				return Metrics.newTimer();
		}

		@Override
		public Timer newWallTimer() {
				if(!recordTraceMetrics) return Metrics.noOpTimer();
				return Metrics.newWallTimer();
		}

		@Override
		public Gauge newMaxGauge() {
				if(!recordTraceMetrics) return Metrics.noOpGauge();
				return Metrics.maxGauge();
		}

		@Override
		public Gauge newMinGauge() {
				if(!recordTraceMetrics) return Metrics.noOpGauge();
				return Metrics.minGauge();
		}

		@Override public boolean isActive() { return recordTraceMetrics; }

		@Override
		public Counter newCounter(){
				if(!recordTraceMetrics) return Metrics.noOpCounter();
				return Metrics.basicCounter();
		}

		public void addScanStartOverride(Row startKey){
				if (scanStartOverride == null){
						scanStartOverride = startKey;
				} else {
						throw new IllegalStateException("A scan start override is already present in this context. Currently " +
										"only one hint can be set.");
				}
		}

		public Row getScanStartOverride(){
				return scanStartOverride;
		}

		public boolean isFirstStepInMultistep() {
				return firstStepInMultistep;
		}

		public void setFirstStepInMultistep(boolean firstStepInMultistep) {
				this.firstStepInMultistep = firstStepInMultistep;
		}

		public byte[] getCurrentTaskId() {
				return currentTaskId;
		}

		public void setCurrentTaskId(byte[] currentTaskId) {
				this.currentTaskId = currentTaskId;
		}

      public byte[] getParentTaskId() { 
         return parentTaskId; 
      }

      public void setParentTaskId(byte[] id) { 
         parentTaskId = id; 
      }

		public void setHashBucket(byte hashBucket) {
				this.hashBucket = hashBucket;
		}

		public byte getHashBucket() {
				return hashBucket;
		}


		public StatementInfo getStatementInfo() {
				return statementInfo;
		}
        public long getStatementId() { return statementId;};
		public boolean shouldRecordTraceMetrics() { return recordTraceMetrics; }

		public KryoPool getKryoPool() {
				return kryoPool;
		}

		public TempTable getTempTable() {
				return tempTable;
		}

		public static enum Side {
				LEFT(0),
				RIGHT(1),
				MERGED(2);

				private final int stateNum;

				private Side(int stateNum) {
						this.stateNum = stateNum;
				}

				public static Side getSide(int stateNum) {
						if (stateNum == LEFT.stateNum)
								return LEFT;
						else if (stateNum == RIGHT.stateNum)
								return RIGHT;
						else if (stateNum == MERGED.stateNum)
								return MERGED;
						else
								throw new IllegalArgumentException("Incorrect stateNum: " + stateNum);
				}
		}

		public static class Path implements Externalizable {
				private int resultSetNumber;
				private Side state;

				public Path() {

				}

				public Path copy() {
						return new Path(this.resultSetNumber, this.state);
				}

				public Path(int resultSetNumber, Side state) {
						this.resultSetNumber = resultSetNumber;
						this.state = state;
				}

				public Path(int resultSetNumber, int state) {
						this.resultSetNumber = resultSetNumber;
						this.state = Side.getSide(state);
				}

				@Override
				public void writeExternal(ObjectOutput out) throws IOException {
						out.writeInt(resultSetNumber);
						out.writeInt(state.stateNum);
				}

				@Override
				public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
						resultSetNumber = in.readInt();
						state = Side.getSide(in.readInt());
				}

				@Override
				public String toString() {
						return String.format(" Path={resultSetNumber=%d, state=%s}", resultSetNumber, state);
				}

		}

}
