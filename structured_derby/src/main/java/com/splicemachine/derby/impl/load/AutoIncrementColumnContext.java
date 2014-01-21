package com.splicemachine.derby.impl.load;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class AutoIncrementColumnContext implements Externalizable {
    private long autoincStart;
	private long autoincInc;
    private byte[] sequenceRowLocation;
    
    public AutoIncrementColumnContext () {}
    
    public AutoIncrementColumnContext (long autoincStart, long autoincInc, byte[] sequenceRowLocation) {
    	this.autoincStart = autoincStart;
    	this.autoincInc = autoincInc;
    	this.sequenceRowLocation = sequenceRowLocation;
    }
    public long getAutoincStart () {
    	return autoincStart;
    }
    public long getAutoincInc () {
    	return autoincInc;
    }
    public byte[] getSequenceRowLocation () {
    	return sequenceRowLocation;
    }
    @Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(autoincStart);
		out.writeLong(autoincInc);
		out.writeInt(sequenceRowLocation.length);
		for(int i = 0; i < sequenceRowLocation.length; i++) {
            out.writeByte(sequenceRowLocation[i]);
		}
		
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		autoincStart = in.readLong();
		autoincInc = in.readLong();
		int length = in.readInt();
		sequenceRowLocation = new byte[length];
		for(int i = 0; i < length; i++) {
			sequenceRowLocation[i] = in.readByte();
		}
		
	}
}
