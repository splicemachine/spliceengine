package com.splicemachine.derby.impl.sql.execute.actions;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.catalog.UUID;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;


/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP INDEX Statement at Execution time.
 *
 */
public class DropIndexConstantAction extends IndexConstantOperation {
	private String				fullIndexName;
	private long				tableConglomerateId;
	/**
	 *	Make the ConstantAction for a DROP INDEX statement.
	 *
	 *
	 *	@param	fullIndexName		Fully qualified index name
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName			Schema that index lives in.
	 *  @param  tableId				UUID for table
	 *  @param  tableConglomerateId	heap Conglomerate Id for table
	 *
	 */
	public DropIndexConstantAction(String fullIndexName,String indexName,String tableName,
		String schemaName, UUID tableId, long tableConglomerateId) {
		super(tableId, indexName, tableName, schemaName);
		this.fullIndexName = fullIndexName;
		this.tableConglomerateId = tableConglomerateId;
	}

	public	String	toString() {
		return "DROP INDEX " + fullIndexName;
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP INDEX.
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction(Activation activation) throws StandardException {
		TableDescriptor td;
		ConglomerateDescriptor cd;
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();

		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		// need to lock heap in exclusive mode first.  Because we can't first
		// shared lock the row in SYSCONGLOMERATES and later exclusively lock
		// it, this is potential deadlock (track 879).  Also td need to be
		// gotten after we get the lock, a concurrent thread could be modifying
		// table shape (track 3804, 3825)

		// older version (or target) has to get td first, potential deadlock
		if (tableConglomerateId == 0)
		{
			td = dd.getTableDescriptor(tableId);
			if (td == null)
			{
				throw StandardException.newException(
					SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
			}
			tableConglomerateId = td.getHeapConglomerateId();
		}
		// XX - TODO NO LOCKING REQUIRED lockTableForDDL(tc, tableConglomerateId, true);
		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
		}

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true) ;

		/* Get the conglomerate descriptor for the index, along
		 * with an exclusive row lock on the row in sys.sysconglomerates
		 * in order to ensure that no one else compiles against the
		 * index.
		 */
		cd = dd.getConglomerateDescriptor(indexName, sd, true);

		if (cd == null)
		{
			throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION, fullIndexName);
		}

		/* Since we support the sharing of conglomerates across
		 * multiple indexes, dropping the physical conglomerate
		 * for the index might affect other indexes/constraints
		 * which share the conglomerate.  The following call will
		 * deal with that situation by creating a new physical
		 * conglomerate to replace the dropped one, if necessary.
		 */
        dropIndex(td,cd);
		dropConglomerate(cd, td, activation, lcc);
		return;
	}
	
    private void dropIndex(TableDescriptor td, ConglomerateDescriptor conglomerateDescriptor) throws StandardException {
    	final long tableConglomId = td.getHeapConglomerateId();
    	final long indexConglomId = conglomerateDescriptor.getConglomerateNumber();

    	//drop the index trigger from the main table
    	HTableInterface mainTable = SpliceAccessManager.getHTable(tableConglomId);
    	try {
    		mainTable.coprocessorExec(SpliceIndexProtocol.class,
    				HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,
    				new Batch.Call<SpliceIndexProtocol, Void>() {
    				@Override
    					public Void call(SpliceIndexProtocol instance) throws IOException {
    						instance.dropIndex(indexConglomId,tableConglomId);
    						return null;
    				}
    		}) ;
    	} catch (Throwable throwable) {
    		throw Exceptions.parseException(throwable);
    	}
    }
}
