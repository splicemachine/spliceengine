package com.splicemachine.test.suites;

/**
 * @author Scott Fines
 * Created on: 2/24/13
 */
public class OperationCategories {

    /**
     * Indicates that a test(or test class is transactional in nature.
     *
     * This will allow ignoring tests that are intent on transactions when
     * transactional tests are a waste of time.
     */
    public static interface Transactional{}

    /**
     * Tests which rely on inserting large volumes of data, and thus might take a very long
     * time to run. Exclude them if you want to speed up run times
     */
    public static interface LargeDataTest{}

}
