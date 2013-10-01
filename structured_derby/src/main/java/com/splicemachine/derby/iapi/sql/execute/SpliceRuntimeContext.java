package com.splicemachine.derby.iapi.sql.execute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class SpliceRuntimeContext implements Externalizable {
	private List<Path> paths = new ArrayList<Path>();
	
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
	
	public void addPath(Path path) {
		paths.add(path);
	}

	public void addPath(int resultSetNumber, int state) {
		addPath(new Path(resultSetNumber,state));
	}
	
	public boolean isLeft(int resultSetNumber) {
		for (Path path: paths) {
			if (path.resultSetNumber == resultSetNumber) {
				if (path.state == 0)
					return true;
				return false;
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




	public static class Path implements Externalizable {
		private int resultSetNumber;
		private int state;
		public Path() {

		}
		
		public Path copy() {
			return new Path(this.resultSetNumber,this.state);
		}
		
		public Path (int resultSetNumber, int state) {
			this.resultSetNumber = resultSetNumber;
			this.state = state;
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeInt(resultSetNumber);
			out.writeInt(state);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			resultSetNumber = in.readInt();
			state = in.readInt();
		}
		@Override
		public String toString() {
			return String.format(" Path={resultSetNumber=%d, state=%d",resultSetNumber, state);
		}
		
	}
	
}
