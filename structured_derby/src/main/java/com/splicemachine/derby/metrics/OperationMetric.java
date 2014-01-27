package com.splicemachine.derby.metrics;

import org.apache.derby.iapi.sql.dictionary.SystemColumn;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public enum OperationMetric {
		/*Total time taken by the operation/task*/
		TOTAL_WALL_TIME(0),
		TOTAL_USER_TIME(1),
		TOTAL_CPU_TIME(2),
		/*Metrics relating to LOCAL scan operations (touching this HBase region)*/
		LOCAL_SCAN_ROWS(3),
		LOCAL_SCAN_BYTES(4),
		LOCAL_SCAN_WALL_TIME(5),
		LOCAL_SCAN_CPU_TIME(6),
		LOCAL_SCAN_USER_TIME(7),
		/*Metrics relating to REMOTE scan operations (touching another HBase region)*/
		REMOTE_SCAN_ROWS(8),
		REMOTE_SCAN_BYTES(9),
		REMOTE_SCAN_WALL_TIME(10),
		REMOTE_SCAN_CPU_TIME(11),
		REMOTE_SCAN_USER_TIME(12),
		/*Metrics relating to Get operations (e.g. index lookups)*/
		REMOTE_GET_ROWS(13),
		REMOTE_GET_BYTES(14),
		REMOTE_GET_WALL_TIME(15),
		REMOTE_GET_CPU_TIME(16),
		REMOTE_GET_USER_TIME(17),
		/*Metrics relating to writing output data for tasks and/or operations like insert*/
		WRITE_ROWS(18),
		WRITE_BYTES(19),
		WRITE_WALL_TIME(20),
		WRITE_CPU_TIME(21),
		WRITE_USER_TIME(22),

		/*General measured values*/
		FILTERED_ROWS(23),  					//number of rows which were filtered or removed
		TASK_QUEUE_WAIT_WALL_TIME(24), //amount of time spent waiting for a task executor thread, or 0 if no tasks involved
		START_TIMESTAMP(25),					//the timestamp at which EXECUTION begins (does not include QUEUE_WAIT_TIME)
		STOP_TIMESTAMP(26),						//the timestamp at which EXECUTION stops (i.e. the last row is written/read. Does NOT include time to transmit task information)
		INPUT_ROWS(27),
		OUTPUT_ROWS(28);

		private final int indexPosition;
		private final int shift;

		OperationMetric(int indexPosition) {
				this.indexPosition = indexPosition;
				this.shift = (1<<(Integer.SIZE-1-indexPosition));
		}

		/*
		 * Note: This may need to be changed to a byte[] if we ever grow beyond 32 enum values.
		 */
		public int encode(int indexInt){
				return indexInt | shift;
		}

		public boolean isSet(int index){
				return (index & shift)==0;
		}

		public int getPosition(){ return indexPosition; }
}

