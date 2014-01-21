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
		private static final String JOBID_NAME = "JOBID" ;

		public String getTableName() {
				return TABLE_NAME;
		}

		/**
		 * @return null if there are no primary keys, else the names
		 * of the primary keys for the table.
		 */
		protected String[] getPrimaryKeys(){
			return new String[]{STATEMENTID_NAME,OPERATIONID_NAME,JOBID_NAME,TASKID_NAME};
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

		protected SystemColumn[] buildColumnList() {
				OperationMetric[] metrics = OperationMetric.values();
				Arrays.sort(metrics, new Comparator<OperationMetric>() {
						@Override
						public int compare(OperationMetric o1, OperationMetric o2) {
								return o1.getPosition()-o2.getPosition();
						}
				});
				SystemColumn[] columns = new SystemColumn[metrics.length+4];
				columns[0] = SystemColumnImpl.getColumn("STATEMENTID",Types.BIGINT,false);
				columns[1] = SystemColumnImpl.getColumn("OPERATIONID",Types.BIGINT,false);
				columns[2] = SystemColumnImpl.getColumn("JOBID",Types.BIGINT,false);
				columns[3] = SystemColumnImpl.getColumn("TASKID",Types.BIGINT,false);
				for(int i=0;i<metrics.length;i++){
						String name = metrics[i].name().replaceAll("_", "");
						columns[i+4] = SystemColumnImpl.getColumn(name, Types.BIGINT,true);
				}
				return columns;
		}

		public static void main(String... args) throws Exception{
				System.out.println(new XPLAINTaskDescriptor().getTableDDL("APP"));
		}
}
