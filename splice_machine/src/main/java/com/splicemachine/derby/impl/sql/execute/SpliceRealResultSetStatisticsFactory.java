/*

   Derby - Class com.splicemachine.db.impl.sql.execute.RealResultSetStatisticsFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.PreparedStatement;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.execute.ResultSetStatistics;
import com.splicemachine.db.iapi.sql.execute.ResultSetStatisticsFactory;
import com.splicemachine.db.iapi.sql.execute.RunTimeStatistics;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.rts.RealAnyResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealDeleteCascadeResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealDeleteResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealDistinctScalarAggregateStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealDistinctScanStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealGroupedAggregateStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealHashJoinStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealHashLeftOuterJoinStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealHashScanStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealHashTableStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealIndexRowToBaseRowStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealInsertResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealMergeSortJoinStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealMergeSortLeftOuterJoinStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealNestedLoopJoinStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealNestedLoopLeftOuterJoinStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealNormalizeResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealOnceResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealProjectRestrictStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealRowCountStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealRowResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealScalarAggregateStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealScrollInsensitiveResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealSortStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealTableScanStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealUnionResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealUpdateResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealVTIStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RealWindowResultSetStatistics;
import com.splicemachine.db.impl.sql.execute.rts.RunTimeStatisticsImpl;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.AnyOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DeleteCascadeOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DeleteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DependentOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DistinctScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DistinctScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashLeftOuterJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashTableOperation;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowToBaseRowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MergeSortJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MergeSortLeftOuterJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.NestedLoopJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.NestedLoopLeftOuterJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.NormalizeOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OnceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ProjectRestrictOperation;
import com.splicemachine.derby.impl.sql.execute.operations.rowcount.RowCountOperation;
import com.splicemachine.derby.impl.sql.execute.operations.RowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScrollInsensitiveOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SortOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.UnionOperation;
import com.splicemachine.derby.impl.sql.execute.operations.UpdateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.VTIOperation;
import com.splicemachine.derby.impl.sql.execute.operations.WindowOperation;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * ResultSetStatisticsFactory provides a wrapper around all of
 * objects associated with run time statistics.
 * <p>
 * This implementation of the protocol is for returning the "real"
 * run time statistics.  We have modularized this so that we could
 * have an implementation that just returns null for each of the
 * objects should we decided to provide a configuration without
 * the run time statistics feature.
 *
 */
public class SpliceRealResultSetStatisticsFactory 
		implements ResultSetStatisticsFactory
{
	private static Logger LOG = Logger.getLogger(SpliceRealResultSetStatisticsFactory.class);
	//
	// ExecutionFactory interface
	//
	//
	// ResultSetStatisticsFactory interface
	//

	/**
		@see ResultSetStatisticsFactory#getRunTimeStatistics
	 */
	public RunTimeStatistics getRunTimeStatistics(
			Activation activation, 
			ResultSet rs,
			NoPutResultSet[] subqueryTrackingArray)
		throws StandardException
	{
		PreparedStatement preStmt = activation.getPreparedStatement();

		// If the prepared statement is null then the result set is being
		// finished as a result of a activation being closed during a recompile.
		// In this case statistics should not be generated.
		if (preStmt == null)
			return null;

		SpliceLogUtils.trace(LOG, "in getRunTimeStatistics, activation.getPreparedStatement()=%s",activation.getPreparedStatement());

		ResultSetStatistics topResultSetStatistics;

		if (rs instanceof NoPutResultSet)
			topResultSetStatistics = getResultSetStatistics((NoPutResultSet) rs);
		else
			topResultSetStatistics = getResultSetStatistics(rs);

		/* Build up the info on the materialized subqueries */
		int subqueryTrackingArrayLength = (subqueryTrackingArray == null) ? 0 : subqueryTrackingArray.length;
		ResultSetStatistics[] subqueryRSS = new ResultSetStatistics[subqueryTrackingArrayLength];
		boolean anyAttached = false;
		for (int index = 0; index < subqueryTrackingArrayLength; index++)
		{
			if (subqueryTrackingArray[index] != null &&
				subqueryTrackingArray[index].getPointOfAttachment() == -1)
			{
				subqueryRSS[index] = getResultSetStatistics(subqueryTrackingArray[index]);
				anyAttached = true;
			}
		}
		if (anyAttached == false)
		{
			subqueryRSS = null;
		}

		// Get the info on all of the materialized subqueries (attachment point = -1)
		return new RunTimeStatisticsImpl(
								preStmt.getSPSName(),
								activation.getCursorName(),
								preStmt.getSource(),
								preStmt.getCompileTimeInMillis(),
								preStmt.getParseTimeInMillis(),
								preStmt.getBindTimeInMillis(),
								preStmt.getOptimizeTimeInMillis(),
								preStmt.getGenerateTimeInMillis(),
								rs.getExecuteTime(),
								preStmt.getBeginCompileTimestamp(),
								preStmt.getEndCompileTimestamp(),
								rs.getBeginExecutionTimestamp(),
								rs.getEndExecutionTimestamp(),
								subqueryRSS,
								topResultSetStatistics);
	}

	/**
		@see ResultSetStatisticsFactory#getResultSetStatistics
	 */
	public ResultSetStatistics getResultSetStatistics(ResultSet rs)
	{
		if (!rs.returnsRows())
			return getNoRowsResultSetStatistics(rs);
		else if (rs instanceof NoPutResultSet)
			return getResultSetStatistics((NoPutResultSet) rs);
		else
			return null;
	}

    public ResultSetStatistics getResultSetStatistics(SpliceOperation operation){
        throw new UnsupportedOperationException("Implement!");
    }

	public ResultSetStatistics getNoRowsResultSetStatistics(ResultSet rs)
	{
		ResultSetStatistics retval = null;

		/* We need to differentiate based on instanceof in order
		 * to find the right constructor to call.  This is ugly,
		 * but if we don't do instanceof then rs is always seen as an
		 * interface instead of a class when we try to overload 
		 * a method with both.
		 */
		if( rs instanceof InsertOperation)
		{
			InsertOperation irs = (InsertOperation) rs;

			retval = new RealInsertResultSetStatistics(
									irs.modifiedRowCount(),
									false,//irs.constants.deferred,
									0,//FIXME: need this after index is implemented irs.constants.irgs.length,
									true,//irs.userSpecifiedBulkInsert,
									true,//irs.bulkInsertPerformed,
									false,//irs.constants.lockMode == TransactionController.MODE_TABLE,
									irs.getExecuteTime(), 
									getResultSetStatistics(irs.savedSource)
									);

			irs.savedSource = null;
		}
		/*else if( rs instanceof InsertVTIResultSet)
		{
			InsertVTIResultSet iVTIrs = (InsertVTIResultSet) rs;

			retval = new RealInsertVTIResultSetStatistics(
									iVTIrs.rowCount,
									iVTIrs.constants.deferred,
									iVTIrs.getExecuteTime(), 
									getResultSetStatistics(iVTIrs.savedSource)
									);

			iVTIrs.savedSource = null;
		}*/
		else if( rs instanceof UpdateOperation)
		{
			UpdateOperation urs = (UpdateOperation) rs;

			retval = new RealUpdateResultSetStatistics(
					urs.modifiedRowCount(),
					false,//urs.constants.deferred,
					0,//FIXME: need this after index is implemented urs.constants.irgs.length,
					false,//urs.constants.lockMode == TransactionController.MODE_TABLE,
					urs.getExecuteTime(), 
					getResultSetStatistics(urs.savedSource)
					);

			urs.savedSource = null;
		}
		else if( rs instanceof DeleteCascadeOperation)
		{
			DeleteCascadeOperation dcrs = (DeleteCascadeOperation) rs;
			int dependentTrackingArrayLength =
				(dcrs.dependentResultSets == null) ? 0 :
					dcrs.dependentResultSets.length;
			ResultSetStatistics[] dependentTrackingArray =
				new ResultSetStatistics[dependentTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < dependentTrackingArrayLength; index++)
			{
				if (dcrs.dependentResultSets[index] != null)
				{
					dependentTrackingArray[index] =
										getResultSetStatistics(
											dcrs.dependentResultSets[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				dependentTrackingArray = null;
			}

			retval = new RealDeleteCascadeResultSetStatistics(
					dcrs.modifiedRowCount(),
					false,//urs.constants.deferred,
					0,//FIXME: need this after index is implemented urs.constants.irgs.length,
					false,//urs.constants.lockMode == TransactionController.MODE_TABLE,
					dcrs.getExecuteTime(), 
					getResultSetStatistics(dcrs.savedSource),
									dependentTrackingArray
									);

			dcrs.savedSource = null;
		}
		else if( rs instanceof DeleteOperation)
		{
			DeleteOperation drs = (DeleteOperation) rs;

			retval = new RealDeleteResultSetStatistics(
					drs.modifiedRowCount(),
					false,//urs.constants.deferred,
					0,//FIXME: need this after index is implemented urs.constants.irgs.length,
					false,//urs.constants.lockMode == TransactionController.MODE_TABLE,
					drs.getExecuteTime(), 
					getResultSetStatistics(drs.savedSource)
									);

			drs.savedSource = null;
		}
		/*else if( rs instanceof DeleteVTIResultSet)
		{
			DeleteVTIResultSet dVTIrs = (DeleteVTIResultSet) rs;

			retval = new RealDeleteVTIResultSetStatistics(
									dVTIrs.modifiedRowCount(),
									dVTIrs.getExecuteTime(), 
									getResultSetStatistics(dVTIrs.savedSource)
									);

			dVTIrs.savedSource = null;
		}*/


		return retval;
	}

	public ResultSetStatistics getResultSetStatistics(NoPutResultSet rs)
	{
		/* We need to differentiate based on instanceof in order
		 * to find the right constructor to call.  This is ugly,
		 * but if we don't do instanceof then rs is always seen as an
		 * interface instead of a class when we try to overload 
		 * a method with both.
		 */
		if( rs instanceof ProjectRestrictOperation)
		{
			ProjectRestrictOperation prrs = (ProjectRestrictOperation) rs;
			int subqueryTrackingArrayLength =
				(prrs.subqueryTrackingArray == null) ? 0 :
					prrs.subqueryTrackingArray.length;
			ResultSetStatistics[] subqueryTrackingArray =
				new ResultSetStatistics[subqueryTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < subqueryTrackingArrayLength; index++)
			{
				if (prrs.subqueryTrackingArray[index] != null &&
					prrs.subqueryTrackingArray[index].getPointOfAttachment() ==
						prrs.getResultSetNumber())
				{
					subqueryTrackingArray[index] =
										getResultSetStatistics(
											prrs.subqueryTrackingArray[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				subqueryTrackingArray = null;
			}

			return new RealProjectRestrictStatistics(
											prrs.numOpens,
											prrs.inputRows,
											prrs.rowsFiltered,
											prrs.constructorTime,
											prrs.openTime,
											prrs.nextTime,
											prrs.closeTime,
											prrs.getResultSetNumber(),
											0,
											0,
											subqueryTrackingArray,
											(prrs.getRestrictionMethodName() != null),
											prrs.doesProjection(),
											prrs.getEstimatedRowCount(),
											prrs.getEstimatedCost(),
											getResultSetStatistics(prrs.getSource())
											);
		}
		else if (rs instanceof RowCountOperation)
		{
			RowCountOperation rcrs = (RowCountOperation) rs;

			return new RealRowCountStatistics(
				rcrs.numOpens,
				rcrs.inputRows,
				rcrs.rowsFiltered,
				rcrs.constructorTime,
				rcrs.openTime,
				rcrs.nextTime,
				rcrs.closeTime,
				rcrs.getResultSetNumber(),
				rcrs.getEstimatedRowCount(),
				rcrs.getEstimatedCost(),
				getResultSetStatistics(rcrs.getSource()) );
		}
		else if (rs instanceof SortOperation)
		{
			SortOperation srs = (SortOperation) rs;
			
			return new RealSortStatistics(
											srs.numOpens,
											srs.inputRows,
											srs.rowsFiltered,
											srs.constructorTime,
											srs.openTime,
											srs.nextTime,
											srs.closeTime,
											srs.getResultSetNumber(),
											(int)srs.getRowsInput(),
											(int)srs.getRowsOutput(),
											srs.needsDistinct(),
											false,//srs.isInSortedOrder,
											srs.getSortProperties(),
											srs.getEstimatedRowCount(),
											srs.getEstimatedCost(),
											getResultSetStatistics(srs.getSource())
										);
		}
		else if (rs instanceof DistinctScalarAggregateOperation)
		{
			DistinctScalarAggregateOperation dsars = (DistinctScalarAggregateOperation) rs;

			return new RealDistinctScalarAggregateStatistics(
											dsars.numOpens,
											dsars.inputRows,
											dsars.rowsFiltered,
											dsars.constructorTime,
											dsars.openTime,
											dsars.nextTime,
											dsars.closeTime,
											dsars.getResultSetNumber(),
											(int)dsars.getRowsInput(),
											dsars.getEstimatedRowCount(),
											dsars.getEstimatedCost(),
											getResultSetStatistics(dsars.getSource())
										);
		}
		else if (rs instanceof ScalarAggregateOperation)
		{
			ScalarAggregateOperation sars = (ScalarAggregateOperation) rs;

			return new RealScalarAggregateStatistics(
											sars.numOpens,
											sars.inputRows,
											sars.rowsFiltered,
											sars.constructorTime,
											sars.openTime,
											sars.nextTime,
											sars.closeTime,
											sars.getResultSetNumber(),
											sars.isSingleInputRow(),
											(int)sars.getRowsInput(),
											sars.getEstimatedRowCount(),
											sars.getEstimatedCost(),
											getResultSetStatistics(sars.getSource())
										);
		}
		else if (rs instanceof GroupedAggregateOperation)
		{
			GroupedAggregateOperation gars = (GroupedAggregateOperation) rs;
			
			return new RealGroupedAggregateStatistics(
											gars.numOpens,
											gars.inputRows,
											gars.rowsFiltered,
											gars.constructorTime,
											gars.openTime,
											gars.nextTime,
											gars.closeTime,
											gars.getResultSetNumber(),
											(int)gars.getRowsInput(),
											gars.hasDistinctAggregate(),
											gars.isInSortedOrder(),
											gars.getSortProperties(),
											gars.getEstimatedRowCount(),
											gars.getEstimatedCost(),
											getResultSetStatistics(gars.getSource())
										);
		}
		else if (rs instanceof TableScanOperation)
		{
			boolean instantaneousLocks = false;
			TableScanOperation tsrs = (TableScanOperation) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;
			String lockRequestString = null;
			
			switch (tsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_READ_UNCOMMITTED);
					break;
			}

			if (tsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
												SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (tsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_ROW);
					break;
			}

			if (tsrs.getIndexName() != null)
			{
				/* Start and stop position strings will be non-null
			 	* if the TSRS has been closed.  Otherwise, we go off
			 	* and build the strings now.
			 	*/
				startPosition = tsrs.printStartPosition();
				stopPosition = tsrs.printStopPosition();
			}

			return new 
                RealTableScanStatistics(
                    tsrs.numOpens,
                    tsrs.inputRows,
                    tsrs.rowsFiltered,
                    tsrs.constructorTime,
                    tsrs.openTime,
                    tsrs.nextTime,
                    tsrs.closeTime,
                    tsrs.getResultSetNumber(),
                    tsrs.getTableName(),
					tsrs.userSuppliedOptimizerOverrides,
                    tsrs.getIndexName(),
                    tsrs.isConstraint,
                    SpliceBaseOperation.printQualifiers(tsrs.getScanQualifiers()),
                    tsrs.getScanProperties(),
                    startPosition,
                    stopPosition,
                    isolationLevel,
                    lockRequestString,
                    tsrs.rowsPerRead,
                    false,//tsrs.coarserLock,
                    tsrs.getEstimatedRowCount(),
                    tsrs.getEstimatedCost());
		}

		/*else if (rs instanceof LastIndexKeyResultSet )
		{
			LastIndexKeyResultSet lrs = (LastIndexKeyResultSet) rs;
			String isolationLevel =  null;
			String lockRequestString = null;

			switch (lrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_UNCOMMITTED);
                    break;
			}

			switch (lrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					lockRequestString = MessageService.getTextMessage(
													SQLState.LANG_SHARE_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					lockRequestString = MessageService.getTextMessage(
													SQLState.LANG_SHARE_ROW);
					break;
			}

			return new RealLastIndexKeyScanStatistics(
											lrs.numOpens,
											lrs.constructorTime,
											lrs.openTime,
											lrs.nextTime,
											lrs.closeTime,
											lrs.getResultSetNumber(),
											lrs.tableName,
											lrs.indexName,
											isolationLevel,
											lockRequestString,
											lrs.getEstimatedRowCount(),
											lrs.getEstimatedCost());
		}*/
		else if (rs instanceof HashLeftOuterJoinOperation)
		{
			HashLeftOuterJoinOperation hlojrs = (HashLeftOuterJoinOperation) rs;

			return new RealHashLeftOuterJoinStatistics(
											hlojrs.numOpens,
											hlojrs.inputRows,
											hlojrs.rowsFiltered,
											hlojrs.constructorTime,
											hlojrs.openTime,
											hlojrs.nextTime,
											hlojrs.closeTime,
											hlojrs.getResultSetNumber(),
											hlojrs.getLeftNumCols(),
											hlojrs.getRightNumCols(),
											hlojrs.rowsReturned,
											hlojrs.restrictionTime,
											hlojrs.getEstimatedRowCount(),
											hlojrs.getEstimatedCost(),
											hlojrs.getUserSuppliedOptimizerOverrides(),
											getResultSetStatistics(hlojrs.getLeftResultSet()),
											getResultSetStatistics(hlojrs.getRightResultSet()),
											hlojrs.emptyRightRowsReturned);
		}
		else if (rs instanceof NestedLoopLeftOuterJoinOperation)
		{
			NestedLoopLeftOuterJoinOperation nllojrs =
				(NestedLoopLeftOuterJoinOperation) rs;

			return new RealNestedLoopLeftOuterJoinStatistics(
											nllojrs.numOpens,
											nllojrs.inputRows,
											nllojrs.rowsFiltered,
											nllojrs.constructorTime,
											nllojrs.openTime,
											nllojrs.nextTime,
											nllojrs.closeTime,
											nllojrs.getResultSetNumber(),
											nllojrs.getLeftNumCols(),
											nllojrs.getRightNumCols(),
											nllojrs.rowsReturned,
											nllojrs.restrictionTime,
											nllojrs.getEstimatedRowCount(),
											nllojrs.getEstimatedCost(),
											nllojrs.getUserSuppliedOptimizerOverrides(),
											getResultSetStatistics(nllojrs.getLeftResultSet()),
											getResultSetStatistics(nllojrs.getRightResultSet()),
											nllojrs.emptyRightRowsReturned);
		}
		else if (rs instanceof HashJoinOperation)
		{
			HashJoinOperation hjrs = (HashJoinOperation) rs;

			return new RealHashJoinStatistics(
											hjrs.numOpens,
											hjrs.inputRows,
											hjrs.rowsFiltered,
											hjrs.constructorTime,
											hjrs.openTime,
											hjrs.nextTime,
											hjrs.closeTime,
											hjrs.getResultSetNumber(),
											hjrs.getLeftNumCols(),
											hjrs.getRightNumCols(),
											hjrs.rowsReturned,
											hjrs.restrictionTime,
											hjrs.isOneRowRightSide(),
											hjrs.getEstimatedRowCount(),
											hjrs.getEstimatedCost(),
											hjrs.getUserSuppliedOptimizerOverrides(),
											getResultSetStatistics(hjrs.getLeftResultSet()),
											getResultSetStatistics(hjrs.getRightResultSet())
											);
		}
		else if (rs instanceof NestedLoopJoinOperation)
		{
			NestedLoopJoinOperation nljrs = (NestedLoopJoinOperation) rs;

			return new RealNestedLoopJoinStatistics(
											nljrs.numOpens,
											nljrs.inputRows,
											nljrs.rowsFiltered,
											nljrs.constructorTime,
											nljrs.openTime,
											nljrs.nextTime,
											nljrs.closeTime,
											nljrs.getResultSetNumber(),
											nljrs.getLeftNumCols(),
											nljrs.getRightNumCols(),
											nljrs.rowsReturned,
											nljrs.restrictionTime,
											nljrs.isOneRowRightSide(),
											nljrs.getEstimatedRowCount(),
											nljrs.getEstimatedCost(),
											nljrs.getUserSuppliedOptimizerOverrides(),
											getResultSetStatistics(nljrs.getLeftResultSet()),
											getResultSetStatistics(nljrs.getRightResultSet())
											);
		}
		else if (rs instanceof MergeSortJoinOperation)
		{
			MergeSortJoinOperation msjrs = (MergeSortJoinOperation) rs;

			return new RealMergeSortJoinStatistics(
					msjrs.numOpens,
					msjrs.inputRows,
					msjrs.rowsFiltered,
					msjrs.constructorTime,
					msjrs.openTime,
					msjrs.nextTime,
					msjrs.closeTime,
					msjrs.getResultSetNumber(),
					msjrs.getLeftNumCols(),
					msjrs.getRightNumCols(),
					msjrs.rowsReturned,
					msjrs.restrictionTime,
					msjrs.isOneRowRightSide(),
					msjrs.getEstimatedRowCount(),
					msjrs.getEstimatedCost(),
					msjrs.getUserSuppliedOptimizerOverrides(),
					getResultSetStatistics(msjrs.getLeftResultSet()),
					getResultSetStatistics(msjrs.getRightResultSet())
			);
		}
		else if (rs instanceof MergeSortLeftOuterJoinOperation)
		{
			MergeSortLeftOuterJoinOperation mslojrs = (MergeSortLeftOuterJoinOperation) rs;

			return new RealMergeSortLeftOuterJoinStatistics(
					mslojrs.numOpens,
					mslojrs.inputRows,
					mslojrs.rowsFiltered,
					mslojrs.constructorTime,
					mslojrs.openTime,
					mslojrs.nextTime,
					mslojrs.closeTime,
					mslojrs.getResultSetNumber(),
					mslojrs.getLeftNumCols(),
					mslojrs.getRightNumCols(),
					mslojrs.rowsReturned,
					mslojrs.restrictionTime,
					mslojrs.getEstimatedRowCount(),
					mslojrs.getEstimatedCost(),
					mslojrs.getUserSuppliedOptimizerOverrides(),
					getResultSetStatistics(mslojrs.getLeftResultSet()),
					getResultSetStatistics(mslojrs.getRightResultSet()),
					mslojrs.emptyRightRowsReturned);
		}
		else if (rs instanceof IndexRowToBaseRowOperation)
		{
			IndexRowToBaseRowOperation irtbrrs = (IndexRowToBaseRowOperation) rs;

			return new RealIndexRowToBaseRowStatistics(
											irtbrrs.numOpens,
											irtbrrs.inputRows,
											irtbrrs.rowsFiltered,
											irtbrrs.constructorTime,
											irtbrrs.openTime,
											irtbrrs.nextTime,
											irtbrrs.closeTime,
											irtbrrs.getResultSetNumber(),
											irtbrrs.getIndexName(),
											irtbrrs.getAccessedHeapCols(),
											irtbrrs.getEstimatedRowCount(),
											irtbrrs.getEstimatedCost(),
											getResultSetStatistics(irtbrrs.getSource())
											);
		}
		else if (rs instanceof RowOperation)
		{
			RowOperation rrs = (RowOperation) rs;

			return new RealRowResultSetStatistics(
											rrs.numOpens,
											rrs.inputRows,
											rrs.rowsFiltered,
											rrs.constructorTime,
											rrs.openTime,
											rrs.nextTime,
											rrs.closeTime,
											rrs.getResultSetNumber(),
											rrs.getRowsReturned(),
											rrs.getEstimatedRowCount(),
											rrs.getEstimatedCost());
		}
		else if (rs instanceof WindowOperation)
		{
			WindowOperation wrs = (WindowOperation) rs;

			return new RealWindowResultSetStatistics(
											wrs.numOpens,
											wrs.inputRows,
											wrs.rowsFiltered,
											wrs.constructorTime,
											wrs.openTime,
											wrs.nextTime,
											wrs.closeTime,
											wrs.getResultSetNumber(),
											wrs.getEstimatedRowCount(),
											wrs.getEstimatedCost(),
											getResultSetStatistics(wrs.getSource())
											);
		}
		/*else if (rs instanceof SetOpResultSet)
		{
			SetOpResultSet srs = (SetOpResultSet) rs;

			return new RealSetOpResultSetStatistics(
											srs.getOpType(),
											srs.numOpens,
											srs.inputRows,
											srs.rowsFiltered,
											srs.constructorTime,
											srs.openTime,
											srs.nextTime,
											srs.closeTime,
											srs.getResultSetNumber(),
											srs.getRowsSeenLeft(),
											srs.getRowsSeenRight(),
											srs.getRowsReturned(),
											srs.getEstimatedRowCount(),
											srs.getEstimatedCost(),
											getResultSetStatistics(srs.getLeftSourceInput()),
											getResultSetStatistics(srs.getRightSourceInput())
											);
		}*/
		else if (rs instanceof UnionOperation)
		{
			UnionOperation urs = (UnionOperation)rs;

			return new RealUnionResultSetStatistics(
											urs.numOpens,
											urs.inputRows,
											urs.rowsFiltered,
											urs.constructorTime,
											urs.openTime,
											urs.nextTime,
											urs.closeTime,
											urs.getResultSetNumber(),
											urs.rowsSeenLeft,
											urs.rowsSeenRight,
											urs.rowsReturned,
											urs.getEstimatedRowCount(),
											urs.getEstimatedCost(),
											getResultSetStatistics(urs.firstResultSet),
											getResultSetStatistics(urs.secondResultSet)
											);
		}
		else if (rs instanceof AnyOperation)
		{
			AnyOperation ars = (AnyOperation) rs;

			return new RealAnyResultSetStatistics(
											ars.numOpens,
											ars.inputRows,
											ars.rowsFiltered,
											ars.constructorTime,
											ars.openTime,
											ars.nextTime,
											ars.closeTime,
											ars.getResultSetNumber(),
											ars.subqueryNumber,
											ars.pointOfAttachment,
											ars.getEstimatedRowCount(),
											ars.getEstimatedCost(),
											getResultSetStatistics(ars.source)
											);
		}
		else if (rs instanceof OnceOperation)
		{
			OnceOperation ors = (OnceOperation) rs;

			return new RealOnceResultSetStatistics(
											ors.numOpens,
											ors.inputRows,
											ors.rowsFiltered,
											ors.constructorTime,
											ors.openTime,
											ors.nextTime,
											ors.closeTime,
											ors.getResultSetNumber(),
											ors.subqueryNumber,
											ors.pointOfAttachment,
											ors.getEstimatedRowCount(),
											ors.getEstimatedCost(),
											getResultSetStatistics(ors.source)
											);
		}
		else if (rs instanceof NormalizeOperation)
		{
			NormalizeOperation nrs = (NormalizeOperation) rs;

			return new RealNormalizeResultSetStatistics(
											nrs.numOpens,
											nrs.inputRows,
											nrs.rowsFiltered,
											nrs.constructorTime,
											nrs.openTime,
											nrs.nextTime,
											nrs.closeTime,
											nrs.getResultSetNumber(),
											nrs.getEstimatedRowCount(),
											nrs.getEstimatedCost(),
											getResultSetStatistics(nrs.getSource())
											);
		}
		/*else if (rs instanceof MaterializedResultSet)
		{
			MaterializedResultSet mrs = (MaterializedResultSet) rs;

			return new RealMaterializedResultSetStatistics(
											mrs.numOpens,
											mrs.inputRows,
											mrs.rowsFiltered,
											mrs.constructorTime,
											mrs.openTime,
											mrs.nextTime,
											mrs.closeTime,
											mrs.createTCTime,
											mrs.fetchTCTime,
											mrs.getResultSetNumber(),
											mrs.getEstimatedRowCount(),
											mrs.getEstimatedCost(),
											getResultSetStatistics(mrs.source)
											);
		}*/
		else if (rs instanceof ScrollInsensitiveOperation)
		{
			ScrollInsensitiveOperation sirs = (ScrollInsensitiveOperation) rs;

			return new RealScrollInsensitiveResultSetStatistics(
											sirs.numOpens,
											sirs.inputRows,
											sirs.rowsFiltered,
											sirs.constructorTime,
											sirs.openTime,
											sirs.nextTime,
											sirs.closeTime,
											0,//sirs.numFromHashTable,
											0,//sirs.numToHashTable,
											sirs.getResultSetNumber(),
											sirs.getEstimatedRowCount(),
											sirs.getEstimatedCost(),
											getResultSetStatistics(sirs.getSource())
											);
		}
		/*else if (rs instanceof CurrentOfResultSet)
		{
			CurrentOfResultSet cors = (CurrentOfResultSet) rs;

			return new RealCurrentOfStatistics(
											cors.numOpens,
											cors.inputRows,
											cors.rowsFiltered,
											cors.constructorTime,
											cors.openTime,
											cors.nextTime,
											cors.closeTime,
											cors.getResultSetNumber()
											);
		}*/
		else if (rs instanceof HashScanOperation)
		{
			boolean instantaneousLocks = false;
			HashScanOperation hsrs = (HashScanOperation) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;

			switch (hsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

			}

			if (hsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
													SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (hsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockString = lockString + " " +
									MessageService.getTextMessage(
														SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockString = lockString + " " +
									MessageService.getTextMessage(
															SQLState.LANG_ROW);
					break;
			}

			if (hsrs.getIndexName() != null)
			{
				/* Start and stop position strings will be non-null
			 	* if the HSRS has been closed.  Otherwise, we go off
			 	* and build the strings now.
			 	*/
				startPosition = hsrs.printStartPosition();
				stopPosition = hsrs.printStopPosition();
			}
			
			// DistinctScanResultSet is simple sub-class of
			// HashScanResultSet
			if (rs instanceof DistinctScanOperation)
			{
				return new RealDistinctScanStatistics(
											hsrs.numOpens,
											hsrs.inputRows,
											hsrs.rowsFiltered,
											hsrs.constructorTime,
											hsrs.openTime,
											hsrs.nextTime,
											hsrs.closeTime,
											hsrs.getResultSetNumber(),
											hsrs.getTableName(),
											hsrs.getIndexName(),
											hsrs.isConstraint,
											0,//hsrs.hashtableSize,
											hsrs.getKeyColumns(),
											SpliceBaseOperation.printQualifiers(hsrs.getScanQualifiers()),
											SpliceBaseOperation.printQualifiers(hsrs.getNextQualifier()),
											hsrs.getScanProperties(),
											startPosition,
											stopPosition,
											isolationLevel,
											lockString,
											hsrs.getEstimatedRowCount(),
											hsrs.getEstimatedCost()
											);
			}
			else
			{
				return new RealHashScanStatistics(
											hsrs.numOpens,
											hsrs.inputRows,
											hsrs.rowsFiltered,
											hsrs.constructorTime,
											hsrs.openTime,
											hsrs.nextTime,
											hsrs.closeTime,
											hsrs.getResultSetNumber(),
											hsrs.getTableName(),
											hsrs.getIndexName(),
											hsrs.isConstraint,
											0,//hsrs.hashtableSize,
											hsrs.getKeyColumns(),
											SpliceBaseOperation.printQualifiers(hsrs.getScanQualifiers()),
											SpliceBaseOperation.printQualifiers(hsrs.getNextQualifier()),
											hsrs.getScanProperties(),
											startPosition,
											stopPosition,
											isolationLevel,
											lockString,
											hsrs.getEstimatedRowCount(),
											hsrs.getEstimatedCost()
											);
			}
		}
		else if (rs instanceof HashTableOperation)
		{
			HashTableOperation htrs = (HashTableOperation) rs;
			int subqueryTrackingArrayLength =
				(htrs.subqueryTrackingArray == null) ? 0 :
					htrs.subqueryTrackingArray.length;
			ResultSetStatistics[] subqueryTrackingArray =
				new ResultSetStatistics[subqueryTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < subqueryTrackingArrayLength; index++)
			{
				if (htrs.subqueryTrackingArray[index] != null &&
					htrs.subqueryTrackingArray[index].getPointOfAttachment() == htrs.getResultSetNumber())
				{
					subqueryTrackingArray[index] = getResultSetStatistics(
											htrs.subqueryTrackingArray[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				subqueryTrackingArray = null;
			}

			return new 
                RealHashTableStatistics(
                    htrs.numOpens,
                    htrs.inputRows,
                    htrs.rowsFiltered,
                    htrs.constructorTime,
                    htrs.openTime,
                    htrs.nextTime,
                    htrs.closeTime,
                    htrs.getResultSetNumber(),
                    htrs.hashtableSize,
                    htrs.keyColumns,
                    HashScanOperation.printQualifiers(
                        htrs.nextQualifiers),
                    htrs.scanProperties,
                    htrs.getEstimatedRowCount(),
                    htrs.getEstimatedCost(),
                    subqueryTrackingArray,
                    getResultSetStatistics(htrs.source)
                    );
		}
		else if (rs instanceof VTIOperation)
		{
			VTIOperation vtirs = (VTIOperation) rs;

			return new RealVTIStatistics(
										vtirs.numOpens,
										vtirs.inputRows,
										vtirs.rowsFiltered,
										vtirs.constructorTime,
										vtirs.openTime,
										vtirs.nextTime,
										vtirs.closeTime,
										vtirs.getResultSetNumber(),
										vtirs.javaClassName,
										vtirs.getEstimatedRowCount(),
										vtirs.getEstimatedCost()
										);
		}

		else if (rs instanceof DependentOperation)
		{
			boolean instantaneousLocks = false;
			DependentOperation dsrs = (DependentOperation) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;
			String lockRequestString = null;

			switch (dsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_READ_UNCOMMITTED);
					break;
			}

			if (dsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
												SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (dsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_ROW);
					break;
			}

			/* Start and stop position strings will be non-null
			 * if the dSRS has been closed.  Otherwise, we go off
			 * and build the strings now.
			 */
			startPosition = dsrs.startPositionString;
			if (startPosition == null)
			{
				startPosition = dsrs.printStartPosition();
			}
			stopPosition = dsrs.stopPositionString;
			if (stopPosition == null)
			{
				stopPosition = dsrs.printStopPosition();
			}
		
			return new 
                RealTableScanStatistics(
                    dsrs.numOpens,
                    dsrs.inputRows,
                    dsrs.rowsFiltered,
                    dsrs.constructorTime,
                    dsrs.openTime,
                    dsrs.nextTime,
                    dsrs.closeTime,
                    dsrs.getResultSetNumber(),
                    dsrs.tableName,
					null,
                    dsrs.indexName,
                    dsrs.isConstraint,
                    dsrs.printQualifiers(),
                    dsrs.getScanProperties(),
                    startPosition,
                    stopPosition,
                    isolationLevel,
                    lockRequestString,
                    dsrs.rowsPerRead,
                    dsrs.coarserLock,
                    dsrs.getEstimatedRowCount(),
                    dsrs.getEstimatedCost());
		}
		else
		{
			return null;
		}
	}

	//
	// class interface
	//
	public SpliceRealResultSetStatisticsFactory() 
	{
	}

}
