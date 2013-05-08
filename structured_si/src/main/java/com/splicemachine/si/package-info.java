/**
 * The SI, or "snapshot isolation" module. Provides a layer on top of HBase with cross-row transactions. It is based on
 * an MVCC scheme and allows each transaction to see a stable snapshot of the database as of the beginning of the
 * transaction.
 */
package com.splicemachine.si;