/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

/**
 *
 * <h2>DDL Process Flow</h2>
 *
 * The actions package in Splice Machine contains ConstantOperations that engage in dictionary changes.
 *
 * The model for dictionary changes follows a 2PC protocol and a reference flow is described below.
 *
 * <h3>Control Node (Coordinator)</h3>
 *
 * <ol>
 * <li>Grab the key dictionary elements
 * <pre>
 *         LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
 *         DataDictionary dd = lcc.getDataDictionary();
 *         DependencyManager dm = dd.getDependencyManager();
 * </pre>
 * </li>
 * <li>Place the dictionary in writing mode.  This elevates the transaction (physically persists to transactional storage)
 * <pre>
 *         dd.startWriting(lcc);
 * </pre>
 * </li>
 * <li>Perform dictionary level validation.  Example below...
 * <pre>
 *         if (columnDescriptor != null)
 *                   throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
 *                       columnDescriptor.getDescriptorType(),
 * </pre>
 * </li>
 * <li>Invalidate the local dependency manager.  This performs dependency invalidation and is required on the
 *     control node to return proper error messages to the client.
 * <pre>
 *        dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);
 * </pre>
 * </li>
 * <li>Prepare DDL Message (Protocol Buffer) to be sent via DDL Notification Mechanism (Zookeeper).  Additional
 *     messages can be created by modifying the DDL.proto file in the splice_protocol package.
 * <pre>
 *       DDLMessage.DDLChange ddlChange = ProtoUtil.createAlterTable(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), (BasicUUID) this.tableId);
 * </pre>
 * </li>
 * <li>Write the message and place the message on the control node with txn attached.  The notifiyMedataChange will
 *    be picked up by each node in the control mechanism (Vote).  The prepareDataDictionaryChange call adds the current
 *    ddl change to a list of DDL changes.  These will either be committed or rolled back when the transaction
 *    manager performs commit or rollback.  This is a classic Two Phase Commit Model driven by the
 *    SpliceTransactionManager.
 * <pre>
 *    tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
 * </pre>
 * </li>
 * </ol>
 *
 * <h3>Individual Splice Nodes (Cohort-Prepare)</h3>
 *
 * <ol>
 * <li>DDL Transport mechanism automatically registers change as an ongoing ddlChange
 * ({@link com.splicemachine.derby.ddl.DDLWatchRefresher DDLWatchRefresher}).
 *     By registering, the DataDictionaryCache will not cache conglomerates, permissions or statements.
 *     This will aid in not allowing uncommitted dictionary elements to be cached.
 * </li>
 * <li>Capture message via listener or DDLAction.
 * <pre>
 *     {@code
 *     public void startChange(DDLChange change) throws StandardException{
 *         if(change.getDdlChangeType()==DDLChangeType.ALTER_TABLE) {
 *             DDLUtils.preAlterTable(change, getDataDictionary(), getDataDictionary().getDependencyManager());
 *         } else if(change.getDdlChangeType()==DDLChangeType.CREATE_TRIGGER) {
 *                   DDLUtils.preCreateTrigger(change, getDataDictionary(), getDataDictionary().getDependencyManager());
 *         }
 *     }
 *
 *    // Example DDL Actions that are registered.
 *    final List<DDLAction> ddlActions = new ArrayList<>();
 *    ddlActions.add(new AddIndexToPipeline());
 *    ddlActions.add(new DropIndexFromPipeline());
 * }
 * </pre>
 * </li>
 * <li>Perform Dependency Management ejection and DataDictionaryCache Ejection.  This is critical because this is
 *     what will remove cached statements and
 *     a host of other dependencies.  This will prevent nodes from having different versions of data in the cache.<br/>
 *     See {@link com.splicemachine.derby.ddl.DDLUtils DDLUtils}
 *
 * <pre>
 *     {@code
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
 *              flushCachesBasedOnTableDescriptor(td,dd);
 *              dm.invalidateFor(td, DependencyManager.DROP_TABLE, transactionResource.getLcc());
 *          }
 *        } catch (Exception e) {
 *            throw StandardException.plainWrapException(e);
 *        }
 *      }
 *      }
 * <pre>
 * </li>
 * </ol>
 *
 * <h3>2PC Receive All Votes Control Node (Coordinator)</h3>
 * <ol>
 *  <li>Perform transactional dictionary deletes.  We could not do this earlier or we would not have been able
 *      to look up objects.  Example ...
 * <pre>
 *      dd.dropColumnDescriptor(td.getUUID(), colInfo.name, tc);
 *      dd.addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
 * </pre>
 * </li>
 * </ol>
 *
 * <h3>Commit/Rollback (Coordinator Commit/Rollback)</h3>
 *
 * Transaction Manager Activity (initiated via savepoint handling or parent transactional handling, not
 *          performed in the ConstantAction directly)
 * <ol>
 *  <li>Commit: Sends Commit Message to each node.  Perform whatever actions need to be performed on commit.
 *      This will remove the ongoingddlchange from the queue and allow caches to populate.
 * </li>
 *  <li>Rollback: Sends Rollback Message to each node.  Perform whatever actions need to be performed on rollback.
 *     This will remove the ongoingddlchange from the queue and allow caches to populate.
 *  </li>
 * </ol>
 * <hr/>
 *
 * <h2>Alter Table DDL Modification</h2>
 * <ol>
 * <li>Drop Column<br/>
 *
 * When you drop a column in Splice Machine, we do not physically move the data to another HBase Table.  We remove
 * the column from the dictionary.  When we read the data, we always supply the columns selected (even in the case of select *).
 * SI Logic will ignore columns when they are not selected in the list of columns.
 * </li>
 * <li>Add Column<br/>
 *
 * When you add a column, you set the storage position of your column from the sequence on the table descriptor and
 * you add the actual position based on normal derby logic (put it at the end).  There is a dictionary representation
 * and a data representation as you alter the table.
 * </li>
 * <li>Alter Column (Perform 1 then 2.)</li>
 * </ol>
 * <hr/>
 *
 * <h3>Example of Alter Table Design</h3>
 * <ul>
 * <li>Dictionary Column (Format: <code>Name:{relational position, storage position}</code>)<br/>
 *  <code>Table:Customer [ (usa_id:{1,1}), (firstName:{2,2}),(lastName:{3,3}),(updateDate:{4,4})}</code>
 * </li>
 * <li>Data Column (Format: <code>Name:{storage position}</code>)<br/>
 *  <code>Table:Customer [ (usa_id:{1}), (firstName:{2}),(lastName:{3}),(updateDate:{4})}</code>
 * </li>
 *
 * <li>Dictionary performs 2PC process outlined above, no data is changed in the table.<br/>
 * <code>alter table Customer drop column lastName</code>
 * <pre>
 *      Dictionary:
 *          Table:Customer [ (usa_id:{1,1}), (firstName:{2,2}),(updateDate:{3,4})}
 *
 *      Data:
 *          Table:Customer [ (usa_id:{1}), (firstName:{2}),(lastName:{3}),(updateDate:{4})}
 * </pre>
 * <br/>
 * <code>alter table Customer add column newLastName varchar(256)</code>
 * <pre>
 *      Dictionary (Notice no reference to data position 3):
 *          Table:Customer [ (usa_id:{1,1}), (firstName:{2,2}),(updateDate:{3,4}), (newLastName:{4,5})}
 *
 *      Data:
 *          Table:Customer [ (usa_id:{1}), (firstName:{2}),(lastName:{3}),(updateDate:{4}),(newLastName:{5})}
 * </pre>
 * </li>
 *  <li>Notice how the dictionary representation will not be able to read <code>lastName</code> since there is no field pointing
 *  to it (Dead Data).  There are edge cases that would require a cleanup mechanism for this data in the
 *  case where significant drop columns occurred.<br/>
 *  See <a href="https://splicemachine.atlassian.net/browse/DB-4450">DB-4450</a>
 *  </li>
 *  <li>Also, altering a table's Primary Keys requires moving the data since PK information is encoded in the row key.<br/>
 *  See <a href="https://splicemachine.atlassian.net/browse/DB-4407">DB-4407</a>
 *  </li>
 * </ul>
 */
package com.splicemachine.derby.impl.sql.execute.actions;
