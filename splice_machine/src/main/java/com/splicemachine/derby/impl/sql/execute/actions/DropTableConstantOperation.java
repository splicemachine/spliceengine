package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DropTableDDLChangeDesc;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import org.apache.derby.catalog.UUID;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.Exceptions;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.store.access.TransactionController;


/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP TABLE Statement at Execution time.
 *
 */

public class DropTableConstantOperation extends DDLSingleTableConstantOperation {
	private final long conglomerateNumber;
	private final String fullTableName;
	private final String tableName;
	private final SchemaDescriptor sd;
	private final boolean cascade; 		
	/**
	 *	Make the ConstantAction for a DROP TABLE statement.
	 *
	 *
	 *	@param	fullTableName		Fully qualified table name
	 *	@param	tableName			Table name.
	 *	@param	sd					Schema that table lives in.
	 *  @param  conglomerateNumber	Conglomerate number for heap
	 *  @param  tableId				UUID for table
	 *  @param  behavior			drop behavior: RESTRICT, CASCADE or default
	 *
	 */
	public DropTableConstantOperation(String fullTableName, String tableName, SchemaDescriptor sd,
								long conglomerateNumber,UUID tableId, int behavior) {
		super(tableId);
		this.fullTableName = fullTableName;
		this.tableName = tableName;
		this.sd = sd;
		this.conglomerateNumber = conglomerateNumber;
		this.cascade = (behavior == StatementType.DROP_CASCADE);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
	}
	public	String	toString() {
		return "DROP TABLE " + fullTableName;
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
      TableDescriptor td;
      ConglomerateDescriptor[] cds;
      LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
      DataDictionary dd = lcc.getDataDictionary();
      DependencyManager dm = dd.getDependencyManager();
      TransactionController tc = lcc.getTransactionExecute();

      /*
       * Inform the data dictionary that we are about to write to it.
       * There are several calls to data dictionary "get" methods here
       * that might be done in "read" mode in the data dictionary, but
       * it seemed safer to do this whole operation in "write" mode.
       *
       * We tell the data dictionary we're done writing at the end of
       * the transaction.
       */
      dd.startWriting(lcc);

      /* Get the table descriptor. */
      td = dd.getTableDescriptor(tableId);

      if (td == null) {
          throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
      }

      long heapId = td.getHeapConglomerateId();

      /* Drop the triggers */
      for (Object aTdl : dd.getTriggerDescriptors(td)) {
          TriggerDescriptor trd = (TriggerDescriptor) aTdl;
          trd.drop(lcc);
      }

      /* Drop all defaults */
      ColumnDescriptorList cdl = td.getColumnDescriptorList();
      int	cdlSize = cdl.size();

      for (int index = 0; index < cdlSize; index++) {
          ColumnDescriptor cd = cdl.elementAt(index);

          // If column has a default we drop the default and
          // any dependencies
          if (cd.getDefaultInfo() != null) {
              DefaultDescriptor defaultDesc = cd.getDefaultDescriptor(dd);
              dm.clearDependencies(lcc, defaultDesc);
          }
      }

      /* Drop the columns */
      dd.dropAllColumnDescriptors(tableId, tc);

      /* Drop all table and column permission descriptors */
      dd.dropAllTableAndColPermDescriptors(tableId, tc);

      /* Drop the constraints */
      dropAllConstraintDescriptors(td, activation);

      /*
       * Drop all the conglomerates.  Drop the heap last, because the
       * store needs it for locking the indexes when they are dropped.
       */
      cds = td.getConglomerateDescriptors();

      long[] dropped = new long[cds.length - 1];
      int numDropped = 0;
      for (ConglomerateDescriptor cd : cds) {
          /*
           * if it's for an index, since similar indexes share one
           * conglomerate, we only drop the conglomerate once
           */
          if (cd.getConglomerateNumber() != heapId) {
              long thisConglom = cd.getConglomerateNumber();

              int i;
              for (i = 0; i < numDropped; i++) {
                  if (dropped[i] == thisConglom)
                      break;
              }
              if (i == numDropped) {
                  //not dropped
                  dropped[numDropped++] = thisConglom;
                  tc.dropConglomerate(thisConglom);
                  dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
              }
          }
      }

      /*
       * Prepare all dependents to invalidate.  (This is there chance
       * to say that they can't be invalidated.  For example, an open
       * cursor referencing a table/view that the user is attempting to
       * drop.) If no one objects, then invalidate any dependent objects.
       * We check for invalidation before we drop the table descriptor
       * since the table descriptor may be looked up as part of
       * decoding tuples in SYSDEPENDS.
       */
      dm.invalidateFor(td, DependencyManager.DROP_TABLE, lcc);

      /* Invalidate dependencies remotely.  */
      DDLChange ddlChange = new DDLChange(((SpliceTransactionManager)tc).getActiveStateTxn(), DDLChangeType.DROP_TABLE);
      ddlChange.setTentativeDDLDesc(new DropTableDDLChangeDesc(this.conglomerateNumber, this.tableId));

      notifyMetadataChangeAndWait(ddlChange);

      // The table itself can depend on the user defined types of its columns.
      // Drop all of those dependencies now.
      adjustUDTDependencies(lcc, dd, td, null, true);

      /* Drop the table */
      dd.dropTableDescriptor(td, sd, tc);

      /* Drop the conglomerate descriptors */
      dd.dropAllConglomerateDescriptors(td, tc);

      /*
       * Drop the store element at last, to prevent dangling reference
       * for open cursor, beetle 4393.
       */
      tc.dropConglomerate(heapId);
    }

    private void dropAllConstraintDescriptors(TableDescriptor td, Activation activation) throws StandardException {
        ConstraintDescriptor				cd;
        ConstraintDescriptorList 			cdl;
        ConstraintDescriptor 				fkcd;
        ConstraintDescriptorList 			fkcdl;
        LanguageConnectionContext			lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();

        cdl = dd.getConstraintDescriptors(td);
		
		    /*
		     * First go, don't drop unique or primary keys.
		     * This will ensure that self-referential constraints
		     * will work ok, even if not cascading.
	 	     */
        for(int index = 0; index < cdl.size(); ) {
		        /* The current element will be deleted underneath
		         * the loop, so we only increment the counter when
		         * skipping an element. (HACK!)
		         */
            cd = cdl.elementAt(index);
            if (cd instanceof ReferencedKeyConstraintDescriptor) {
                index++;
                continue;
            }

            dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
            dropConstraint(cd, td, activation, lcc, true);
        }

		    /*
	 	     * Referenced keys (unique or pk) constraints only
		     */
        while (cdl.size() > 0) {
		        /* The current element will be deleted underneath
		         * the loop. (HACK!)
		         */
            cd = cdl.elementAt(0);
            if (SanityManager.DEBUG) {
                if (!(cd instanceof ReferencedKeyConstraintDescriptor)) {
                    SanityManager.THROWASSERT("Constraint descriptor not an instance of " +
                            "ReferencedKeyConstraintDescriptor as expected.  Is a "+ cd.getClass().getName());
                }
            }

			      /*
			       * Drop the referenced constraint (after we got
			       * the primary keys) now.  Do this prior to
			       * droping the referenced keys to avoid performing
			       * a lot of extra work updating the referencedcount
			       * field of sys.sysconstraints.
			       *
			       * Pass in false to dropConstraintsAndIndex so it
			       * doesn't clear dependencies, we'll do that ourselves.
			       */
            dropConstraint(cd, td, activation, lcc, false);

			      /*
			       * If we are going to cascade, get all the
			       * referencing foreign keys and zap them first.
			       */
            if (cascade) {
				        /*
				         * Go to the system tables to get the foreign keys
				         * to be safe
				         */

                fkcdl = dd.getForeignKeys(cd.getUUID());

				        /*
				         * For each FK that references this key, drop
				         * it.
				         */
                for(int inner = 0; inner < fkcdl.size(); inner++) {
                    fkcd = fkcdl.elementAt(inner);
                    dm.invalidateFor(fkcd, DependencyManager.DROP_CONSTRAINT, lcc);
                    dropConstraint(fkcd, td, activation, lcc, true);
                    activation.addWarning(
                            StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
                                    fkcd.getConstraintName(),
                                    fkcd.getTableDescriptor().getName()));
                }
            }

			      /*
			       * Now that we got rid of the fks (if we were cascading), it is
			       * ok to do an invalidate for.
        		 */
            dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
            dm.clearDependencies(lcc, cd);
        }
    }

}
