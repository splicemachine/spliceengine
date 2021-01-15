package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.stream.function.csv.CsvLineReaderCR;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class CsvLineReaderCRTest {
    void test(String s, String[] expected, int configBufferSize) throws IOException {
        CsvLineReaderCR r = new CsvLineReaderCR(new StringReader(s), 1024, configBufferSize);

        for(int i=0; ; i++) {
            String line = r.readLine();
            if( line == null ) break;
            Assert.assertTrue( i < expected.length );
            Assert.assertEquals(r.getLineNumber(), i+1);
            Assert.assertEquals( expected[i*2], line);
            Assert.assertEquals( expected[i*2+1], r.getCurrentLineEnding());
        }
        Assert.assertEquals( expected.length/2, r.getLineNumber() );

    }
    @Test
    public void testSimple() throws IOException {
        test( "Hello\nWorld\n", new String[]{"Hello", "\n", "World", "\n"}, 1024);
        test( "Hello\r\nWorld\n", new String[]{"Hello", "\r\n", "World", "\n"}, 1024);
        test( "Hello\nWorld\r", new String[]{"Hello", "\n", "World", "\r"}, 1024);
        test( "Hello\r\nWorld\r\n", new String[]{"Hello", "\r\n", "World", "\r\n"}, 1024);
        test( "Hello\rWorld", new String[]{"Hello", "\r", "World", ""}, 1024);
    }

    @Test
    public void testAllCases() throws IOException {
        for (int size = 1; size <= 20; ++size) {
            test("", new String[]{}, size);

            // CASE 1
            // 1234\n
            test("1234\n", new String[]{"1234", "\n"}, size);
            // CASE 2
            // 1234<EOF>
            test("1234", new String[]{"1234", ""}, size);

            // CASE 3
            // 123\r\n123\r\n
            test("123\r\n123\r\nabc\n", new String[]{"123", "\r\n", "123", "\r\n", "abc", "\n"}, size);


            // CASE 4
            // 123\r123\r\n

            test("123\r123\r\nabc\n", new String[]{"123", "\r", "123", "\r\n", "abc", "\n"}, size);

            // CASE 5
            // 1234\r<EOF>
            test("1234\r", new String[]{"1234", "\r"}, size);
            //test("1234\r", new String[]{"1234\r"}, false, 5);

            // CASE 6
            // 1234\r\n
            // abc\r\n
                test("1234\r\nabc\r\n", new String[]{"1234", "\r\n", "abc", "\r\n"}, size);

            // CASE 7
            // 1234\rA
            test( "123456", new String[]{"123456", ""}, size);
            test( "12345", new String[]{"12345", ""}, size);
            test( "1234\rA", new String[]{"1234", "\r", "A", ""}, size);
        }
    }
}
