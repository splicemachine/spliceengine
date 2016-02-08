package com.splicemachine.derby.impl.load;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.junit.Assert;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public class ImportTestUtils{

    private ImportTestUtils(){
    }

    public static void assertRowsEquals(ExecRow correctRow,ExecRow actualRow){
        DataValueDescriptor[] correctRowArray=correctRow.getRowArray();
        DataValueDescriptor[] actualRowArray=actualRow.getRowArray();
        for(int dvdPos=0;dvdPos<correctRow.nColumns();dvdPos++){
            Assert.assertEquals("Incorrect column at position "+dvdPos,
                    correctRowArray[dvdPos],
                    actualRowArray[dvdPos]);

        }
    }

}
