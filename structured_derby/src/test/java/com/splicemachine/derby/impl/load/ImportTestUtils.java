package com.splicemachine.derby.impl.load;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.junit.Assert;

import java.util.Comparator;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public class ImportTestUtils {

    private ImportTestUtils(){}

    public static Comparator<ExecRow> columnComparator(int columnPosition){
        return new ExecRowComparator(columnPosition);
    }

		public static void assertRowsEquals(ExecRow correctRow, ExecRow actualRow)  {
				DataValueDescriptor [] correctRowArray = correctRow.getRowArray();
				DataValueDescriptor [] actualRowArray = actualRow.getRowArray();
				for(int dvdPos=0;dvdPos<correctRow.nColumns();dvdPos++){
						Assert.assertEquals("Incorrect column at position " + dvdPos,
										correctRowArray[dvdPos],
										actualRowArray[dvdPos]);

				}
		}

    private static class ExecRowComparator implements Comparator<ExecRow> {
        private final int colNumber;

        private ExecRowComparator(int colNumber) {
            this.colNumber = colNumber;
        }

        @Override
        public int compare(ExecRow o1, ExecRow o2) {
            if(o1==null){
                if(o2==null) return 0;
                else return -1;
            }else if(o2==null)
                return 1;
            else{
                try{
                    return o1.getColumn(colNumber).compare(o2.getColumn(colNumber));
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
