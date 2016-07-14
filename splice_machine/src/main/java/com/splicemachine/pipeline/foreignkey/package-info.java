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