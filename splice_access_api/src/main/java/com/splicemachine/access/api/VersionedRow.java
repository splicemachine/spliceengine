package com.splicemachine.access.api;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface VersionedRow{

    long commitTsVersion();

    long antiTombstoneVersion();

    long tombstoneVersion();

    long dataVersion();

    long commitTimestamp();

    long counterValue();

    byte[] rowKeyArray();

    int rowKeyOffset();

    int rowKeyLength();

}
