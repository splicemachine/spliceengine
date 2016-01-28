/**
 *
 * <b>DDL Process Flow</b>
 *
 * The actions package in Splice Machine contains ConstantOperations that engage in dictionary changes.
 *
 * The model for dictionary changes follows a 2PC protocol and a reference flow is described below.
 *
 * Control Node (Coordinator):
 *
 * 1.  Grab the key dictionary elements
 *
 *         LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
 *         DataDictionary dd = lcc.getDataDictionary();
 *         DependencyManager dm = dd.getDependencyManager();
 *
 * 2.  Place the dictionary in writing mode.  This elevates the transaction (physically persists to transactional
 *      storage)
 *
 *         dd.startWriting(lcc);
 *
 * 3.  Perform dictionary level validation.  Example below...
 *
 *         if (columnDescriptor != null)
 *                   throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
 *                       columnDescriptor.getDescriptorType(),
 *
 *
 * 4.  Invalidate the local dependency manager.  This performs dependency invalidation and is required on the
 *     control node to return proper error messages to the client.
 *
 *        dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);
 *
 * 5.  Prepare DDL Message (Protocol Buffer) to be sent via DDL Notification Mechanism (Zookeeper).  Additional
 *     messages can be created by modifying the DDL.proto file in the splice_protocol package.
 *
 *       DDLMessage.DDLChange ddlChange = ProtoUtil.createAlterTable(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), (BasicUUID) this.tableId);
 *
 * 6. Write the message and place the message on the control node with txn attached.  The notifiyMedataChange will
 *    be picked up by each node in the control mechanism (Vote).  The prepareDataDictionaryChange call adds the current
 *    ddl change to a list of DDL changes.  These will either be committed or rolled back when the transaction
 *    manager performs commit or rollback.  This is a classic Two Phase Commit Model driven by the
 *    SpliceTransactionManager.
 *
 *    tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
 *
 * Individual Splice Nodes (Cohort-Prepare)
 *
 * 1.  DDL Transport mechanism automatically registers change as an ongoing ddlChange (DDLWatchRefresher).
 *     By registering, the DataDictionaryCache will not cache conglomerates, permissions or statements.
 *     This will aid in not allowing uncommitted dictionary elements to be cached.
 *
 * 2.  Capture message via listener or DDLAction.
 *
 *          @Override
 *           public void startChange(DDLChange change) throws StandardException{
 *               if(change.getDdlChangeType()==DDLChangeType.ALTER_TABLE) {
 *                   DDLUtils.preAlterTable(change, getDataDictionary(), getDataDictionary().getDependencyManager());
 *               }
 *               else if(change.getDdlChangeType()==DDLChangeType.CREATE_TRIGGER) {
 *                   DDLUtils.preCreateTrigger(change, getDataDictionary(), getDataDictionary().getDependencyManager());
 *                }
 *
 *
 *        // Example DDL Actions that are registered.
 *
 *                final List<DDLAction> ddlActions = new ArrayList<>();
 *                ddlActions.add(new AddIndexToPipeline());
 *                ddlActions.add(new DropIndexFromPipeline());
 *
 *
 * 3.  Perform Dependency Management ejection and DataDictionaryCache Ejection.  This is critical because this is
 *     what will remove cached statements and
 *     a host of other dependencies.  This will prevent nodes from having different versions of data in the cache.
 *
 *      public static void preAlterStats(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException{
 *       if (LOG.isDebugEnabled())
 *          SpliceLogUtils.debug(LOG,"preDropTable with change=%s",change);
 *       try {
 *          TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
 *          ContextManager currentCm = ContextService.getFactory().getCurrentContextManager();
 *          SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
 *          transactionResource.prepareContextManager();
 *          transactionResource.marshallTransaction(txn);
 *          List<DerbyMessage.UUID> tdUIDs = change.getAlterStats().getTableIdList();
 *          for (DerbyMessage.UUID uuuid : tdUIDs) {
 *              TableDescriptor td = dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuuid));
 *              if (td==null) // Table Descriptor transaction never committed
 *                  return;
 *          flushCachesBasedOnTableDescriptor(td,dd);
 *          dm.invalidateFor(td, DependencyManager.DROP_TABLE, transactionResource.getLcc());
 *          }
 *          } catch (Exception e) {
 *              throw StandardException.plainWrapException(e);
 *          }
 *       }
 *
 * 2PC Receive All Votes Control Node (Coordinator)
 *
 *  1.  Perform transactional dictionary deletes.  We could not do this earlier or we would not have been able
 *      to look up objects.  Example ...
 *
 *
 *               dd.dropColumnDescriptor(td.getUUID(), colInfo.name, tc);
 *               dd.addDescriptor(columnDescriptor, td,
 *                   DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
 *
 * Commit/Rollback (Coordinator Commit/Rollback)
 *
 * Transaction Manager Activity (initiated via savepoint handling or parent transactional handling, not
 *          performed in the ConstantAction directly)
 *
 *  1.  Commit: Sends Commit Message to each node.  Perform whatever actions need to be performed on commit.
 *      This will remove the ongoingddlchange from the queue and allow caches to populate.
 *
 *  2.  Rollback: Sends Rollback Message to each node.  Perform whatever actions need to be performed on rollback.
 *     This will remove the ongoingddlchange from the queue and allow caches to populate.
 *
 *
 *
 * --------------------------------------------------------------------------------------------------
 *
 *
 * <b>Alter Table DDL Modification</b>
 *
 * 1.  Drop Column
 *
 * When you drop a column in Splice Machine, we do not physically move the data to another HBase Table.  We remove
 * the column from the dictionary.  When we read the data, we always supply the columns selected (even in the case of select *).
 * SI Logic will ignore columns when they are not selected in the list of columns.
 *
 * 2.  Add Column
 *
 * When you add a column, you set the storage position of your column from the sequence on the table descriptor and
 * you add the actual position based on normal derby logic (put it at the end).  There is a dictionary representation
 * and a data representation as you alter the table.
 *
 * 3.  Alter Column (Perform 1 then 2.)
 *
 * Example of Alter Table Design:
 *
 * Dictionary Column Format in example: (Name:{relational position, storage position})
 *
 * Data Column Format in example: (Name:{storage position})
 *
 *      Dictionary:
 *          Table:Customer [ (usa_id:{1,1}), (firstName:{2,2}),(lastName:{3,3}),(updateDate:{4,4})}
 *
 *      Data:
 *          Table:Customer [ (usa_id:{1}), (firstName:{2}),(lastName:{3}),(updateDate:{4})}
 *
 * alter table Customer drop column lastName
 *
 * Dictionary performs 2PC process outlined above, no data is changed in the table.
 *
 *      Dictionary:
 *          Table:Customer [ (usa_id:{1,1}), (firstName:{2,2}),(updateDate:{3,4})}
 *
 *      Data:
 *          Table:Customer [ (usa_id:{1}), (firstName:{2}),(lastName:{3}),(updateDate:{4})}
 *
 *
 * alter table Customer add column newLastName varchar(256)
 *
 *      Dictionary (Notice no reference to data position 3):
 *          Table:Customer [ (usa_id:{1,1}), (firstName:{2,2}),(updateDate:{3,4}), (newLastName:{4,5})}
 *
 *      Data:
 *          Table:Customer [ (usa_id:{1}), (firstName:{2}),(lastName:{3}),(updateDate:{4}),(newLastName:{5})}
 *
 *  Notice how the dictionary representation will not be able to read lastName since there is no field pointing
 *  to it (Dead Data).  There are edge cases that would require a cleanup mechanism for this data in the
 *  case where significant drop columns occurred.  (DB-4450)
 *
 */
package com.splicemachine.derby.impl.sql.execute.actions;