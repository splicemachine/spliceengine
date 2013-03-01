package com.splicemachine.si2.si.api;

/**
 * Generator of transaction timestamps.
 */
public interface IdSource {
	public long nextId();
}
