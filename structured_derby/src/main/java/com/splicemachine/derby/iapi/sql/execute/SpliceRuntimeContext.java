package com.splicemachine.derby.iapi.sql.execute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class SpliceRuntimeContext implements Externalizable {
    private static final long serialVersionUID = 1l;
	private List<Path> paths = new ArrayList<Path>();
    private boolean isSink = false;

	public SpliceRuntimeContext() {
		
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

	public SpliceRuntimeContext copy() {
		SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
		for (Path path: paths) {
			spliceRuntimeContext.addPath(path.copy());
		}
		return spliceRuntimeContext;
	}

    public boolean isSink(){
        return isSink;
    }

	public void addPath(Path path) {
		paths.add(0,path);
	}

	public void addPath(int resultSetNumber, int state) {
		addPath(new Path(resultSetNumber,state));
	}

    public void addPath(int resultSetNumber, Side side){
        addPath(new Path(resultSetNumber,side));
    }

    public Side getPathSide(int resultSetNumber){
        for (Path path: paths) {
            if (path.resultSetNumber == resultSetNumber) {
                return path.state;
            }
        }
        throw new IllegalStateException("No Path found for the specified result set!");
    }

	public boolean isLeft(int resultSetNumber) {
		for (Path path: paths) {
			if (path.resultSetNumber == resultSetNumber) {
                return path.state == Side.LEFT;
            }
		}
		Thread.dumpStack();
		throw new RuntimeException("UnionOperation Not Supported");
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(paths.size());
		for (Path path: paths) {
			out.writeObject(path);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int size = in.readInt();
		paths = new ArrayList<Path>(size);
		for (int i = 0; i<size; i++) {
			paths.add((Path) in.readObject());
		}
	}

	
	
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("SpliceRuntimeContext {  ");
		for (Path path: paths) {
			sb.append(path);
		}
		sb.append(" }");
		return sb.toString();
	}

    public void markAsSink() {
        isSink = true;
    }

    public boolean isSinkOp() {
        return isSink;
    }


    public static enum Side{
        LEFT(0),
        RIGHT(1),
        MERGED(2);

        private final int stateNum;

        private Side(int stateNum) {
            this.stateNum = stateNum;
        }

        public static Side getSide(int stateNum){
            if(stateNum==LEFT.stateNum)
                return LEFT;
            else if(stateNum==RIGHT.stateNum)
                return RIGHT;
            else if(stateNum==MERGED.stateNum)
                return MERGED;
            else
                throw new IllegalArgumentException("Incorrect stateNum: "+ stateNum);
        }
    }

	public static class Path implements Externalizable {
		private int resultSetNumber;
		private Side state;
		public Path() {

		}
		
		public Path copy() {
			return new Path(this.resultSetNumber,this.state);
		}

        public Path(int resultSetNumber, Side state) {
            this.resultSetNumber = resultSetNumber;
            this.state = state;
        }

        public Path (int resultSetNumber, int state) {
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
			return String.format(" Path={resultSetNumber=%d, state=%d",resultSetNumber, state);
		}
		
	}
	
}
