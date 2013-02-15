package org.apache.derby.impl.sql.conn;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derby.impl.sql.GenericStatement;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.PreparedStatement;

import org.apache.derby.iapi.services.cache.Cacheable;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.monitor.Monitor;

/**
*/
public class CachedStatement implements Cacheable {

	private GenericStorablePreparedStatement ps;
	private Object identity;

	public CachedStatement() {
	}

	/**
	 * Get the PreparedStatement that is associated with this Cacheable
	 */
	public GenericStorablePreparedStatement getPreparedStatement() {
		return ps;
	}

	/* Cacheable interface */

	/**

	    @see Cacheable#clean
	*/
	public void clean(boolean forRemove) {
	}

	/**
	*/
	public Cacheable setIdentity(Object key) {

		identity = key;
		ps = new GenericStorablePreparedStatement((GenericStatement) key);
		ps.setCacheHolder(this);

		return this;
	}

	/** @see Cacheable#createIdentity */
	public Cacheable createIdentity(Object key, Object createParameter) {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Not expecting any create() calls");

		return null;

	}

	/** @see Cacheable#clearIdentity */
	public void clearIdentity() {

		if (SanityManager.DEBUG)
			SanityManager.DEBUG("StatementCacheInfo","CLEARING IDENTITY: "+ps.getSource());
		ps.setCacheHolder(null);

		identity = null;
		ps = null;
	}

	/** @see Cacheable#getIdentity */
	public Object getIdentity() {
		return identity;
	}

	/** @see Cacheable#isDirty */
	public boolean isDirty() {
		return false;
	}

	/* Cacheable interface */
}
