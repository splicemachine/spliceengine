package com.splicemachine.derby.impl.load;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.junit.Assert;
import com.google.common.base.Joiner;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.junit.Assert;
import java.util.Comparator;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.fail;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public class ImportTestUtils{

    // WARNING: if you need to add a method to this class, you might need to add it
    // to SpliceUnitTest in engine_it module instead, for proper dependencies.
    // For example, printMsgSQLState and createBadLogDirectory were moved there.

    private ImportTestUtils(){}

    public static Comparator<ExecRow> columnComparator(int columnPosition){
        return new ExecRowComparator(columnPosition);
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

    public static void assertBadFileContainsError(File directory, String errorFile, String errorCode) throws IOException {
        printBadFile(directory, errorFile, errorCode);
    }

    public static String printBadFile(File directory, String fileName) throws IOException {
        return printBadFile(directory, fileName, null);
    }

    public static String printBadFile(File directory, String fileName, String errorCode) throws IOException {
        // look for file in the "baddir" directory with same name as import file ending in ".bad"
        Path path = Paths.get(fileName);
        File badFile = new File(directory, path.getFileName() + ".bad");
        if (badFile.exists()) {
//            System.err.println(badFile.getAbsolutePath());
            List<String> badLines = Files.readAllLines(badFile.toPath(), Charset.defaultCharset());
            if (errorCode != null && ! errorCode.isEmpty()) {
                // make sure at least one error entry contains the errorCode
                boolean found = false;
                for (String line : badLines) {
                    if (line.startsWith(errorCode)) {
                        found = true;
                        break;
                    }
                }
                if (! found ) {
                    fail("Didn't find expected SQLState '"+errorCode+"' in bad file: "+badFile.getCanonicalPath());
                }
            }
            return "Error file contents: "+badLines.toString();
        }
        return "File does not exist: "+badFile.getCanonicalPath();
    }

    public static String printImportFile(File directory, String fileName) throws IOException {
        File file = new File(directory, fileName);
        if (file.exists()) {
            List<String> badLines = new ArrayList<>();
            for(String line : Files.readAllLines(file.toPath(), Charset.defaultCharset())) {
                badLines.add("{"+line+"}");
            }
            return "File contents: "+badLines.toString();
        }
        return "File does not exist: "+file.getCanonicalPath();
    }

    public static class ResultList {
        private List<List<String>> resultList = new ArrayList<>();
        private List<List<String>> expectedList = new ArrayList<>();

        public static ResultList create() {
            return new ResultList();
        }

        public ResultList toFileRow(String... values) {
            if (values != null && values.length > 0) {
                resultList.add(Arrays.asList(values));
            }
            return this;
        }

        public ResultList expected(String... values) {
            if (values != null && values.length > 0) {
                expectedList.add(Arrays.asList(values));
            }
            return this;
        }

        public ResultList fill(PrintWriter pw) {
            for (List<String> row : resultList) {
                pw.println(Joiner.on(",").join((row)));
            }
            return this;
        }

        public int nRows() {
            return resultList.size();
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            for (List<String> row : expectedList) {
                buf.append(row.toString()).append("\n");
            }
            return buf.toString();
        }
    }
}
