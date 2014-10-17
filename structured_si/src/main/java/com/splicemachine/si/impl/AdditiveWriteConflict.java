package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Thrown whenever an Additive conflict is detected which cannot be ignored (i.e. during an UPSERT
 * process).
 *
 * @author Scott Fines
 *         Date: 10/17/14
 */
public class AdditiveWriteConflict extends DoNotRetryIOException{
}
