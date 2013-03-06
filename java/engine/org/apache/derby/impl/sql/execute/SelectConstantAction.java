package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class SelectConstantAction  implements ConstantAction {
    private int[] pkCols;

    //no-op
    public void executeConstantAction(Activation activation) throws StandardException { }

    public SelectConstantAction(int[] pkCols){
        this.pkCols = pkCols;
    }

    public int[] getKeyColumns(){
        return pkCols;
    }

}
