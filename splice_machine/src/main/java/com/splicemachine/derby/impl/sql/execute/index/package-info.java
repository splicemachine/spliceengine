/**
 * Tools relating to Index manipulation in SpliceSQLEngine.
 *
 * Indexing is managed transparently through the use of Coprocessors.
 * Specifically, the SpliceIndexProtocol and the BatchProtocol(discussed elsewhere).
 *
 * These intercept Puts and Deletes and insure that the corresponding actions are properly
 * carried over into the relevant indices.
 *
 * This particular package contains management tools around explicit index management.
 */
package com.splicemachine.derby.impl.sql.execute.index;