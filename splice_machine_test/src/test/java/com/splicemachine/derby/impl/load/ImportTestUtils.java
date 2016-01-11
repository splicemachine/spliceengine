package com.splicemachine.derby.impl.load;

import static org.junit.Assert.assertTrue;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.homeless.TestUtils;

import org.junit.Assert;

import java.io.File;
import java.sql.SQLException;
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

    public static String printMsgSQLState(String testName, SQLException e) {
        // useful for debugging import errors
        StringBuilder buf =new StringBuilder(testName);
        buf.append("\n");
        int i =1;
        SQLException child = e;
        while (child != null) {
            buf.append(i++).append(" ").append(child.getSQLState()).append(" ")
               .append(child.getLocalizedMessage()).append("\n");
            child = child.getNextException();
        }
        return buf.toString();
    }

    public static File createBadLogDirectory(String schemaName) {
        File badImportLogDirectory = new File(SpliceUnitTest.getBaseDirectory()+"/target/BAD/"+schemaName);
        if (badImportLogDirectory.exists()) {
            //noinspection ConstantConditions
            for (File file : badImportLogDirectory.listFiles()) {
                assertTrue("Couldn't create "+file,file.delete());
            }
            assertTrue("Couldn't create "+badImportLogDirectory,badImportLogDirectory.delete());
        }
        assertTrue("Couldn't create "+badImportLogDirectory,badImportLogDirectory.mkdirs());
        assertTrue("Failed to create "+badImportLogDirectory,badImportLogDirectory.exists());
        return badImportLogDirectory;
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
