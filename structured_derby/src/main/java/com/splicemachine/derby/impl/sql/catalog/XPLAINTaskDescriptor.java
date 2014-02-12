package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.metrics.OperationMetric;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;
import org.apache.derby.impl.sql.catalog.XPLAINTableDescriptor;

import java.sql.Types;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Scott Fines
 * Date: 1/21/14
 */
public class XPLAINTaskDescriptor {
		private static final String TABLE_NAME = "SYSXPLAIN_TASKHISTORY";
		private static final String TASKID_NAME = "TASKID";
		private static final String OPERATIONID_NAME = "OPERATIONID";
		private static final String STATEMENTID_NAME = "STATEMENTID";

		public String getTableName() {
				return TABLE_NAME;
		}

		/**
		 * @return null if there are no primary keys, else the names
		 * of the primary keys for the table.
		 */
		protected String[] getPrimaryKeys(){
			return new String[]{STATEMENTID_NAME,OPERATIONID_NAME,TASKID_NAME};
		}

		public final String getTableDDL(String schemaName){
				String escapedSchema = IdUtil.normalToDelimited(schemaName);
				String escapedTableName = IdUtil.normalToDelimited(getTableName());
				StringBuilder queryBuilder = new StringBuilder("create table ");
				queryBuilder = queryBuilder.append(escapedSchema).append(".").append(escapedTableName);
				queryBuilder = queryBuilder.append("(");

				SystemColumn []cols = buildColumnList();
				boolean isFirst=true;
				for(SystemColumn col:cols){
						if(!isFirst)queryBuilder = queryBuilder.append(",");
						else isFirst=false;

						queryBuilder = queryBuilder.append(col.getName()).append(" ");
						queryBuilder = queryBuilder.append(col.getType().getCatalogType().getSQLstring());
						if(!col.getType().isNullable()){
								queryBuilder = queryBuilder.append(" NOT NULL");
						}
				}
				String[] pks = getPrimaryKeys();
				if(pks!=null){
						queryBuilder = queryBuilder.append(", PRIMARY KEY(");

						isFirst=true;
						for(String pk:pks){
								if(!isFirst)queryBuilder = queryBuilder.append(",");
								else isFirst=false;
								queryBuilder = queryBuilder.append(pk);
						}
						queryBuilder = queryBuilder.append(")");
				}
				queryBuilder = queryBuilder.append(")");
				return queryBuilder.toString();
		}

		public static final String getOperationDetailView(String schemaName){
				//TODO -sf- generalize
				String opHistoryView = "create view "+schemaName+".SYSXPLAIN_OPERATION_DETAIL as "+ SYSXPLAIN_OPERATION_DETAIL_BASE_SQL+"from "+ schemaName+".SYSXPLAIN_OPERATIONHISTORY oh,"+schemaName+".SYSXPLAIN_TASKHISTORY th "+SYSXPLAIN_OPERATION_DETAIL_POSTFIX;
				return opHistoryView;
		}

		protected SystemColumn[] buildColumnList() {
				OperationMetric[] metrics = OperationMetric.values();
				Arrays.sort(metrics, new Comparator<OperationMetric>() {
						@Override
						public int compare(OperationMetric o1, OperationMetric o2) {
								return o1.getPosition()-o2.getPosition();
						}
				});
				SystemColumn[] columns = new SystemColumn[metrics.length+6];
				columns[0] = SystemColumnImpl.getColumn("STATEMENTID",Types.BIGINT,false);
				columns[1] = SystemColumnImpl.getColumn("OPERATIONID",Types.BIGINT,false);
				columns[2] = SystemColumnImpl.getColumn("TASKID",Types.BIGINT,false);
				columns[3] = SystemColumnImpl.getColumn("HOST",Types.VARCHAR,false,32642);
				columns[4] = SystemColumnImpl.getColumn("REGION",Types.VARCHAR,true,32462);
				for(int i=0;i<metrics.length;i++){
						String name = metrics[i].name().replaceAll("_", "");
						columns[i+5] = SystemColumnImpl.getColumn(name, Types.BIGINT,true);
				}
				columns[metrics.length+5] = SystemColumnImpl.getColumn("BUFFER_FILLRATIO",Types.DOUBLE,true);
				return columns;
		}

		public static void main(String... args) throws Exception{
				System.out.println(new XPLAINTaskDescriptor().getTableDDL("APP"));
		}

		private static final String SYSXPLAIN_OPERATION_DETAIL_BASE_SQL =
						"select" +
										"	oh.statementId," +
										"	oh.operationId," +
										"	oh.operation_type," +
										"	oh.jobcount," +
										"	oh.taskcount," +
										"	oh.failedtaskcount," +
										"	count(distinct th.region) as REGIONCOUNT," +
										"	count(distinct th.host) as NUM_REGIONSERVERS," +
										"	sum(th.totalwalltime)/cast(1000000000 as double) as TOTALWALLTIME_S," +
										"	sum(th.totalcputime)/cast(1000000000 as double) as TOTALCPUTIME_S," +
										"	sum(th.totalusertime)/cast(1000000000 as double) as TOTALUSERTIME_S," +
										"	sum(th.localscanrows) as TOTAL_LOCAL_SCAN_ROWS," +
										"	sum(th.localscanbytes) as TOTAL_LOCAL_SCAN_BYTES," +
										"	sum(th.localscanwalltime)/cast(1000000000 as double) as LOCAL_SCAN_WALL_TIME_S," +
										"	sum(th.localscancputime)/cast(1000000000 as double) as LOCAL_SCAN_CPU_TIME_S," +
										"	sum(th.localscanusertime)/cast(1000000000 as double) as LOCAL_SCAN_USER_TIME_S," +
										"	sum((th.localscancputime-th.localscanusertime))/cast(1000*1000*1000 as double) as LOCAL_SCAN_SYSTEM_TIME_S," +
										"	sum(th.remotescanrows) as TOTAL_REMOTE_SCAN_ROWS," +
										"	sum(th.remotescanbytes) as TOTAL_REMOTE_SCAN_BYTES," +
										"	sum(th.remotescanwalltime)/cast(1000000000 as double) as REMOTE_SCAN_WALL_TIME_S," +
										"	sum(th.remotescancputime)/cast(1000000000 as double) as REMOTE_SCAN_CPU_TIME_S," +
										"	sum(th.remotescanusertime)/cast(1000000000 as double) as REMOTE_SCAN_USER_TIME_S," +
										"	sum((th.remotescancputime-th.remotescanusertime))/cast(1000*1000*1000 as double) as REMOTE_SCAN_SYSTEM_TIME_S," +
										"	sum(th.remotegetrows) as TOTAL_REMOTE_GET_ROWS," +
										"	sum(th.remotegetbytes) as TOTAL_REMOTE_GET_BYTES," +
										"	sum(th.remotegetwalltime)/cast(1000000000 as double) as REMOTE_GET_WALL_TIME_S," +
										"	sum(th.remotegetcputime)/cast(1000000000 as double) as REMOTE_GET_CPU_TIME_S," +
										"	sum(th.remotegetusertime)/cast(1000000000 as double) as REMOTE_GET_USER_TIME_S," +
										"	sum((th.remotegetcputime-th.remotegetusertime))/cast(1000*1000*1000 as double) as REMOTE_GET_SYSTEM_TIME_S," +
										"	sum(th.writerows) as TOTAL_ROWS_WRITTEN," +
										"	sum(th.writebytes) as TOTAL_BYTES_WRITTEN," +
										"	sum(th.retriedwriteattempts) as TOTAL_WRITE_ATTEMPTS," +
										"	sum(th.rejectedwriteattempts) as TOTAL_REJECTED_WRITES," +
										"	sum(th.failedwriteattempts) as TOTAL_FAILED_WRITES," +
										"	sum(th.partialwritefailures) as TOTAL_PARTIAL_WRITE_FAILURES," +
										"	sum(th.processingwalltime)/cast(1000000000 as double) as PROCESSING_WALL_TIME_S," +
										"	sum(th.processingcputime)/cast(1000000000 as double) as PROCESSING_CPU_TIME_S," +
										"	sum(th.processingusertime)/cast(1000000000 as double) as PROCESSING_USER_TIME_S," +
										"	sum((th.processingcputime-th.processingusertime))/cast(1000*1000*1000 as double) as PROCESSING_SYSTEM_TIME_S," +
										"	sum(th.writesleepwalltime)/cast(1000000000 as double) as WRITE_SLEEP_WALL_TIME_S," +
										"	sum(th.writesleepcputime)/cast(1000000000 as double) as WRITE_SLEEP_CPU_TIME_S," +
										"	sum(th.writesleepusertime)/cast(1000000000 as double) as WRITE_SLEEP_USER_TIME_S," +
										"	sum(th.writenetworkwalltime)/cast(1000000000 as double) as WRITE_NETWORK_WALL_TIME_S," +
										"	sum(th.writenetworkcputime)/cast(1000000000 as double) as WRITE_NETWORK_CPU_TIME_S," +
										"	sum(th.writenetworkusertime)/cast(1000000000 as double) as WRITE_NETWORK_USER_TIME_S," +
										"	sum(th.filteredrows) as ROWS_FILTERED," +
										"	sum(th.taskqueuewaitwalltime)/cast(1000000000 as double) as TOTAL_QUEUE_TIME_S," +
										"	max(th.buffer_fillratio) as MAX_BUFFER_FILL_RATIO," +
										"	min(th.buffer_fillratio) as MIN_BUFFER_FILL_RATIO," +
										"	avg(th.buffer_fillratio) as AVG_BUFFER_FILL_RATIO ";
//										"from " +
//										"	?.SYSXPLAIN_OPERATIONHISTORY oh," +
//										"	?.SYSXPLAIN_TASKHISTORY th " +
						private static final String SYSXPLAIN_OPERATION_DETAIL_POSTFIX=
										"where " +
										"	oh.operationId = th.operationId " +
										"group by " +
										"	oh.statementId," +
										"	oh.operationid," +
										"	oh.operation_type," +
										"	oh.jobcount," +
										"	oh.taskcount," +
										"	oh.failedtaskcount";
}
