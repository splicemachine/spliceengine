package com.splicemachine.derby.impl.load;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

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
