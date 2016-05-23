package com.splicemachine.db.shared.common.udt;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/28/15.
 *
 * Base calss for User defined data types and user defined aggregators. Splice does not try to serialize or deserialize
 * its subclass using kryo.
 */
public class UDTBase implements Externalizable {

    public UDTBase() {}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
