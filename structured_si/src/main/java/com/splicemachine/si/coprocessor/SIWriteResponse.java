package com.splicemachine.si.coprocessor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SIWriteResponse implements Externalizable {
	protected long numberWritten = 0;
	protected long numberDeleted = 0;
	protected enum ResponseStatus {SUCCESS,ERROR,WWCONFLICT}
	protected ResponseStatus responseStatus;
	public SIWriteResponse() {
		super();
	}
	@Override
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		numberWritten = in.readLong();
		numberDeleted = in.readLong();
		responseStatus = ResponseStatus.values()[in.readInt()];
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(numberWritten);
		out.writeLong(numberDeleted);
		out.writeInt(responseStatus.ordinal());
	}

}