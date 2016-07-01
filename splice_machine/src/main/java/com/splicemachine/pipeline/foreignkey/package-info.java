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