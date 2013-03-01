package com.splicemachine.si2.relations.api;

/**
 * Generate timestamps representing the time in milliseconds since the epoch.
 */
public interface Clock {
	long getTime();
}
