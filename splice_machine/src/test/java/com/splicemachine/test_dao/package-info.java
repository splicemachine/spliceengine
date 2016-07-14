/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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