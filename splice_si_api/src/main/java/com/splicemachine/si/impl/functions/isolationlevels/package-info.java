/**
 *
 *
 *
 * READ_COMMITTED, // Read Committed data, no blocking on data in flight
 * READ_UNCOMMITTED, // Read uncommitted data, no blocking on data in flight.
 * SNAPSHOT_ISOLATION, // Snapshot Isolation
 * SELECT_FOR_UPDATE, // Read and Lock data for update, block when reading cell is active (Expensive)
 * SELECT_FOR_UPDATE_WITH_NO_LOCK, // Block when reading cell is active (Slow, used for indexes).
 * SNAPSHOT_ISOLATION_ADDITIVE // Allows for writes to ignore write-write conflicts (Upsert)
 }
 *
 *
 *
 *
 */
package com.splicemachine.si.impl.functions.isolationlevels;