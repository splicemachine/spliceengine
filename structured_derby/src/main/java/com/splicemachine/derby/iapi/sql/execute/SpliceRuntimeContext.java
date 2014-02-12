package com.splicemachine.derby.iapi.sql.execute;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.stats.*;

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
		private String xplainSchema;

		public SpliceRuntimeContext() {
				this.hashBucket = SpliceDriver.driver().getTempTable().getCurrentSpread().bucket((int) System.currentTimeMillis());
		}

		public static SpliceRuntimeContext generateLeftRuntimeContext(int resultSetNumber) {
				SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
				spliceRuntimeContext.addPath(resultSetNumber, 0);
				return spliceRuntimeContext;
		}

		public static SpliceRuntimeContext generateRightRuntimeContext(int resultSetNumber) {
				SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
				spliceRuntimeContext.addPath(resultSetNumber, 1);
				return spliceRuntimeContext;
		}

		public static SpliceRuntimeContext generateSinkRuntimeContext(boolean firstStepInMultistep) {
				SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
				spliceRuntimeContext.firstStepInMultistep = firstStepInMultistep;
				return spliceRuntimeContext;
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
				SpliceRuntimeContext copy = new SpliceRuntimeContext();
				for (IntCursor path : paths.keys()) {
						copy.addPath(path.value, paths.get(path.value));
				}
				copy.scanStartOverride = scanStartOverride;
				copy.hashBucket = hashBucket;
				copy.isSink = isSink;
				copy.currentTaskId = currentTaskId;
				copy.statementInfo = statementInfo;
				copy.xplainSchema = xplainSchema;
				copy.recordTraceMetrics = recordTraceMetrics;
				return copy;
		}


		public void setStatementInfo(StatementInfo statementInfo){
				this.statementInfo = statementInfo;
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
    }


    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("SpliceRuntimeContext {  ");
        sb.append(", hashBucket=");
        sb.append(hashBucket);
        sb.append(", isSink=");
        sb.append(isSink);
        sb.append(" }");
        return sb.toString();
    }

		public void markAsSink() { isSink = true; }

		public boolean isSink() { return isSink; }

		public void recordTraceMetrics(){ this.recordTraceMetrics = true; }

		@Override
		public Timer newTimer(){
				if(!recordTraceMetrics) return Metrics.noOpTimer();
				return Metrics.samplingTimer(SpliceConstants.sampleTimingSize);
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

		public void setHashBucket(byte hashBucket) {
				this.hashBucket = hashBucket;
		}

		public byte getHashBucket() {
				return hashBucket;
		}


		public StatementInfo getStatementInfo() {
				return statementInfo;
		}
		public boolean shouldRecordTraceMetrics() { return recordTraceMetrics; }
		public String getXplainSchema() { return xplainSchema; }
		public void setXplainSchema(String xplainSchema) {
				this.xplainSchema = xplainSchema;
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
						return String.format(" Path={resultSetNumber=%d, state=%d", resultSetNumber, state);
				}

		}

}
