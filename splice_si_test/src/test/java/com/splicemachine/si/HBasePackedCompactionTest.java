package com.splicemachine.si;

import org.junit.Ignore;

/**
 * @author Scott Fines
 *         Date: 2/18/14
 */
@Ignore("was not run by suites")
public class HBasePackedCompactionTest extends HBaseCompactionTest {
    static {
        CLASS_NAME = HBasePackedCompactionTest.class.getSimpleName();
    }

    public HBasePackedCompactionTest() {
        super();
    }


}
