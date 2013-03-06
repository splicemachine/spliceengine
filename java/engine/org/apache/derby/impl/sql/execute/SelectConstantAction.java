package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class SelectConstantAction  implements ConstantAction,Externalizable {
    private static final long serialVersionUID = 1l;
    private int[] pkCols;

    public SelectConstantAction() { }

    //no-op
    public void executeConstantAction(Activation activation) throws StandardException { }

    public SelectConstantAction(int[] pkCols){
        this.pkCols = pkCols;
    }

    public int[] getKeyColumns(){
        return pkCols;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        ArrayUtil.writeIntArray(out,pkCols);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        pkCols = ArrayUtil.readIntArray(in);
    }

}

