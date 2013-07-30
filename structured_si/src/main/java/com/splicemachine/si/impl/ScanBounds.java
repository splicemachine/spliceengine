package com.splicemachine.si.impl;

/**
 * Holder for the largest and smallest keys that need to be included in a scan.
 */
public class ScanBounds<Data> {
    Data minKey;
    Data maxKey;
}
