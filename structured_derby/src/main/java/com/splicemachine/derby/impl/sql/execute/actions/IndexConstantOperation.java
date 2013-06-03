package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public abstract class IndexConstantOperation extends DDLSingleTableConstantOperation {
	private static final Logger LOG = Logger.getLogger(IndexConstantOperation.class);
	public String indexName;
	public String tableName;
	public String schemaName;

	/**
	 *	Make the ConstantAction for a CREATE/DROP INDEX statement.
	 *
	 *	@param	tableId				The table uuid
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName					Schema that index lives in.
	 *
	 */
	protected	IndexConstantOperation(UUID tableId,
								String indexName,
								String tableName,
								String schemaName) {
		super(tableId);
		this.indexName = indexName;
		this.tableName = tableName;
		this.schemaName = schemaName;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
	}

    // CLASS METHODS

	/**
	  *	Get the index name.
	  *
	  *	@return	the name of the index
	  */
    public	String	getIndexName() { 
    	SpliceLogUtils.trace(LOG, "getIndexName %s",indexName);
    	return indexName; 
    }

	/**
	 * Set the index name at execution time.
	 * Useful for unnamed constraints which have a backing index.
	 *
	 * @param indexName		The (generated) index name.
	 */
	public void setIndexName(String indexName) {
    	SpliceLogUtils.trace(LOG, "setIndexName %s",indexName);		
		this.indexName = indexName;
	}
}
