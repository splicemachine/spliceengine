/**
 * This package contains data access objects (DAOs) for use in test cases.  The DAOs in this package should have these
 * characteristics:
 *
 * <ul>
 *     <li>They are passed a java.sql.Connection in their constructor</li>
 *     <li>They never close the connection -- that is left to code at a higher level, test case, or test case watcher</li>
 *     <li>Their methods generally do NOT throw checked exceptions for convenience</li>
 * </ul>
 *
 * Place complex logic for manipulating/querying the database, common to multiple tests, here rather than in
 * TestWatchers.  We want:
 *
 * TestCase -> TestWatcher -> TestDAO
 * TestCase -> TestDAO
 *
 * NOT:
 *
 * TestCase -> TestWatcher (where the TestWatchers do everything under the sun).
 */
package com.splicemachine.test_dao;