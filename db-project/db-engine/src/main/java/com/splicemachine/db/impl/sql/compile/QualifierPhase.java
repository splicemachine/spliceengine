package com.splicemachine.db.impl.sql.compile;

/**
 *
 * Breaks qualifiers by phases.
 *
 * BASE - > Qualifier is going against a key value in a way where splice can perform partitioning.
 * FILTER_BASE -> Qualifier for base table but does not restrict I/O from base scan.  This is evaluated prior to a possible lookup.
 * FILTER_PROJECTION -> Qualifer applied after possible lookup in the projection node on top of the base conglomerate scan.
 *
 */
public enum QualifierPhase {
    BASE,FILTER_BASE,FILTER_PROJECTION
}
