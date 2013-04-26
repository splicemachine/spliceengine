package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVParser;
import com.splicemachine.homeless.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SplitLineReaderTest {

    private static FileSystem fs;

    @BeforeClass
    public static void setup() throws IOException{
        fs = FileSystem.getLocal(new Configuration());
    }

    private FSDataInputStream openFile(String filePath) throws IOException{
        return fs.open(new Path(TestUtils.getClasspathResource(filePath).getPath()));
    }

    @Test
    public void testOneLineFromFile() throws IOException {
        FSDataInputStream is = openFile("order_line_small.csv");

        BufferedReader reader =  SplitLineReader.createBufferedLineReader(is, 0, 2);
        String line1 = reader.readLine();
        String line2 = reader.readLine();

        Assert.assertEquals("\"10058_325\",\"10058\",\"325\",\"2008-01-01 00:00:00\",\"10\",\"0\",\"1\",\"15\",\"13.4667\",\"0\",\"9102\"", line1);
        Assert.assertNull(line2);
    }

    @Test
    public void testReadWholeFile() throws IOException {
        FSDataInputStream is = openFile("small_msdatasample/item.csv");

        BufferedReader reader =  SplitLineReader.createBufferedLineReader(is, 0, Integer.MAX_VALUE);
        List<String> pks = new ArrayList<String>();
        String line = null;
        CSVParser csvParse = new CSVParser(',','"');
        while( (line = reader.readLine()) != null){
            pks.add(csvParse.parseLine(line)[0]);
        }

        Assert.assertEquals(10, pks.size());
        Assert.assertEquals("7", pks.get(0));
        Assert.assertEquals("25", pks.get(1));
        Assert.assertEquals("65", pks.get(2));
        Assert.assertEquals("192", pks.get(3));
        Assert.assertEquals("244", pks.get(4));
        Assert.assertEquals("274", pks.get(5));
        Assert.assertEquals("315", pks.get(6));
        Assert.assertEquals("323", pks.get(7));
        Assert.assertEquals("325", pks.get(8));
        Assert.assertEquals("336", pks.get(9));
    }

    @Test
    public void testBeginMidLine() throws IOException {
        FSDataInputStream is = openFile("small_msdatasample/item.csv");

        BufferedReader reader =  SplitLineReader.createBufferedLineReader(is, 2, Integer.MAX_VALUE);

        List<String> pks = new ArrayList<String>();
        String line = null;
        CSVParser csvParse = new CSVParser(',','"');

        while( (line = reader.readLine()) != null){
            pks.add(csvParse.parseLine(line)[0]);
        }

        Assert.assertEquals(9, pks.size());
        Assert.assertEquals("25", pks.get(0));
        Assert.assertEquals("65", pks.get(1));
        Assert.assertEquals("192", pks.get(2));
        Assert.assertEquals("244", pks.get(3));
        Assert.assertEquals("274", pks.get(4));
        Assert.assertEquals("315", pks.get(5));
        Assert.assertEquals("323", pks.get(6));
        Assert.assertEquals("325", pks.get(7));
        Assert.assertEquals("336", pks.get(8));
    }
}
