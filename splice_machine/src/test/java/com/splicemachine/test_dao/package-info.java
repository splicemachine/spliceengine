/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
