package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.management.StatementInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class SpliceRuntimeContext<Row> implements Externalizable {
    private static final long serialVersionUID = 1l;
    private List<Path> paths = new ArrayList<Path>();
    private boolean isSink;
    private Row scanStartOverride;
    private byte[] currentTaskId;
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

    
    public SpliceRuntimeContext copy() {
        SpliceRuntimeContext copy = new SpliceRuntimeContext();
        for (Path path : paths) {
            copy.addPath(path.copy());
        }
        copy.scanStartOverride = scanStartOverride;
        copy.hashBucket = hashBucket;
        copy.isSink = isSink;
        copy.currentTaskId = currentTaskId;
				copy.statementInfo = statementInfo;
        return copy;
    }

		public void setStatementInfo(StatementInfo statementInfo){
				this.statementInfo = statementInfo;
		}

    public void addPath(Path path) {
        paths.add(0, path);
    }

    public void addPath(int resultSetNumber, int state) {
        addPath(new Path(resultSetNumber, state));
    }

    public void addPath(int resultSetNumber, Side side) {
        addPath(new Path(resultSetNumber, side));
    }

    public Side getPathSide(int resultSetNumber) {
        for (Path path : paths) {
            if (path.resultSetNumber == resultSetNumber) {
                return path.state;
            }
        }
        throw new IllegalStateException("No Path found for the specified result set!");
    }

    public boolean hasPathForResultset(int resultSetNumber) {
        for (Path path : paths) {
            if (path.resultSetNumber == resultSetNumber)
                return true;
        }
        return false;
    }

    public boolean isLeft(int resultSetNumber) {
        for (Path path : paths) {
            if (path.resultSetNumber == resultSetNumber) {
                return path.state == Side.LEFT;
            }
        }
        return false;
    }

    public void markAsSink() {
        isSink = true;
    }

    public boolean isSink() {
        return isSink;
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(paths.size());
        for (Path path : paths) {
            out.writeObject(path);
        }
        out.writeByte(hashBucket);
        out.writeBoolean(firstStepInMultistep);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        paths = new ArrayList<Path>(size);
        for (int i = 0; i < size; i++) {
            paths.add((Path) in.readObject());
        }
        hashBucket = in.readByte();
        firstStepInMultistep = in.readBoolean();
    }


    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("SpliceRuntimeContext {  ");
        for (Path path : paths) {
            sb.append(path);
        }
        sb.append(", hashBucket=");
        sb.append(hashBucket);
        sb.append(", isSink=");
        sb.append(isSink);
        sb.append(" }");
        return sb.toString();
    }

		public StatementInfo getStatementInfo() {
				return statementInfo;
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
