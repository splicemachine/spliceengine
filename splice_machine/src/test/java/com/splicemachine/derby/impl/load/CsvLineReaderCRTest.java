package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.stream.function.csv.CsvLineReaderCR;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class CsvLineReaderCRTest {
    void test(String s, String[] expected, boolean skipCarriageReturnIn0D0A, boolean crNewline, int configBufferSize) throws IOException {
        CsvLineReaderCR r = new CsvLineReaderCR(new StringReader(s), skipCarriageReturnIn0D0A, crNewline,
                1024, configBufferSize);

        for(int i=0; ; i++) {
            String line = r.readLine();
            if( line == null ) break;
            Assert.assertTrue( i < expected.length );
            Assert.assertEquals(r.getLineNumber(), i+1);
            Assert.assertEquals( expected[i], line);
        }
        Assert.assertEquals( expected.length, r.getLineNumber() );

    }
    @Test
    public void test1() throws IOException {
        test( "Hello\nWorld\n", new String[]{"Hello", "World"}, true, false, 1024);
        test( "Hello\nWorld\n", new String[]{"Hello", "World"}, false, false,1024);
    }
    @Test
    public void test2() throws IOException {
        test( "Hello\r\nWorld\r\n", new String[]{"Hello", "World"}, true, false,1024);
        test( "Hello\r\nWorld\r\n", new String[]{"Hello\r", "World\r"}, false, false,1024);
    }

    @Test
    public void test3() throws IOException {
        boolean crNewline = true;
        test("", new String[]{}, true, false,5);

        // CASE 1 : skipCarriageReturnIn0D0A=true/false, configBufferSize=10
        // 1234\n
        test("1234\n", new String[]{"1234"}, true, false,10);
        // CASE 2 : skipCarriageReturnIn0D0A=true/false, configBufferSize=10
        // 1234<EOF>
        test("1234", new String[]{"1234"}, true, false,10);

        // CASE 3 : skipCarriageReturnIn0D0A=true, configBufferSize=20
        // 123\r\n123\r\n
        test("123\r\n123\r\nabc\n", new String[]{"123", "123", "abc"}, true, false,20);
        test("123\r\n123\r\nabc\n", new String[]{"123\r", "123\r", "abc"}, false, false,20);


        // CASE 4 : skipCarriageReturnIn0D0A=true, configBufferSize=20
        // 123\r123\r\n

        test("123\r123\r\nabc\n", new String[]{"123", "123", "abc"}, true, true, 20);
        test("123\r123\r\nabc\n", new String[]{"123\r123\r", "abc"}, false, true, 20);
        test("123\r123\r\nabc\n", new String[]{"123\r123", "abc"}, true, false, 20);
        test("123\r123\r\nabc\n", new String[]{"123\r123\r", "abc"}, false, false, 20);

        // CASE 5 : skipCarriageReturnIn0D0A=true, configBufferSize=5
        // 1234\r<EOF>
        test("1234\r", new String[]{"1234"}, true, false, 5);
        test("1234\r", new String[]{"1234\r"}, false, false, 5);
        //test("1234\r", new String[]{"1234\r"}, false, 5);

        // CASE 6 : skipCarriageReturnIn0D0A=true, configBufferSize=5
        // 1234\r\n
        // abc\r\n
        test("1234\r\nabc\r\n", new String[]{"1234", "abc"}, true, false,5);
        test("1234\r\nabc\r\n", new String[]{"1234\r", "abc\r"}, false, false,5);

        // CASE 7 : skipCarriageReturnIn0D0A=true, configBufferSize=5
        // 1234\rA
    }
    @Test    public void test4() throws IOException {
        test( "1234\rA", new String[]{"1234", "A"}, true, false,5);
        //test( "1234\rA", new String[]{"1234\rA"}, true, 5);
    }
}
