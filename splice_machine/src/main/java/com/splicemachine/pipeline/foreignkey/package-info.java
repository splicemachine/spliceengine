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
 *
 * Foreign Key Handling
 *
 * Strategy
 *
 * Child Writes buffer all parent checks and narrow them down to distinct checks.  These checks
 * are performed in parallel against the parent only after the write occurs to the children.  The
 * check algorithm is conservative in checking both the actual value (Snapshot Isolation) and any values in
 * flight (Read Uncommittted).
 *
 * Parent Deletes immediately check for children elements prior to writing their delete.  The check
 * algorithm is conservative in checking both the actual value (Snapshot Isolation) and any values in
 * flight (Read Uncommittted) utilizing the foreign key index of the child element.  It would be nice in
 * the future if foreign keys did not require an index be created.
 *
 *
 */
package com.splicemachine.pipeline.foreignkey;
