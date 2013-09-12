package com.splicemachine.client.workday;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

/**
 * @author Jeff Cunningham
 *         Date: 8/8/13
 */
public class OmsLogTable extends SpliceTableWatcher {
    public static final String TABLE_NAME = "OMSLOG";

    public static final String INDEX_WHDATE_IDX = "WHDATEIDX";
    public static final String INDEX_WHDATE_IDX_DEF = "(swh_date)";             // type is date

    public static final String INDEX_SYSUSERID_IDX = "SYSTEMUSERIDIDX";
    public static final String INDEX_SYSUSERID_IDX_DEF = "(system_user_id)";    // type is varchar(10)

    public static final String INDEX_HTTPREQ_IDX = "HTTPREQIDX";
    public static final String INDEX_HTTPREQ_IDX_DEF = "(http_request)";        // type is varchar(100)

    public static final String INDEX_HTTPRESP_IDX = "HTTPRESPIDX";
    public static final String INDEX_HTTPRESP_IDX_DEF = "(http_response)";      // type is integer

    public static final String CREATE_STRING = "("+
            "host varchar(30),"+
            "date_time timestamp,"+
            "duration integer,"+
            "http_request varchar(100),"+
            "http_response integer,"+
            "bytes_returned integer,"+
            "transaction_id varchar(10),"+
            "total_time integer,"+
            "task_oms_time integer,"+
            "parse_task_oms_time integer,"+
            "parse_task_total_time integer,"+
            "parse_task_active_count integer,"+
            "parse_task_queue_length integer,"+
            "read_or_update char(1),"+
            "update_task_oms_time integer,"+
            "update_task_total_time integer,"+
            "update_task_active_count integer,"+
            "update_task_queue_length integer,"+
            "response_task_oms_time integer,"+
            "response_task_total_time integer,"+
            "response_task_active_count integer,"+
            "response_task_queue_length integer,"+
            "response_ser integer,"+
            "validation_time integer,"+
            "cache_creates integer,"+
            "cache_clears integer,"+
            "cache_hits bigint,"+
            "cache_misses bigint,"+
            "cache_evicts integer,"+
            "instances_accessed integer,"+
            "decompression_cache_hits bigint,"+
            "decompressions integer,"+
            "offload_count integer,"+
            "offload_requests integer,"+
            "offload_cache_hits integer,"+
            "gi_calls integer,"+
            "sql_read_count integer,"+
            "sql_read_time integer,"+
            "sql_read_time_max integer,"+
            "sql_update_count integer,"+
            "sql_update_time integer,"+
            "sql_update_time_max integer,"+
            "tenant_id varchar(10),"+
            "system_user_id varchar(10),"+
            "task_id varchar(10),"+
            "task_display_name varchar(12),"+
            "session_id varchar(30),"+
            "jsession_id varchar(40),"+
            "request_id varchar(40),"+
            "request_handler varchar(40),"+
            "swh_date date,"+
            "swh_dc varchar(30),"+
            "swh_server varchar(30),"+
            "swh_app varchar(30),"+
            "swh_env varchar(30))";

    public OmsLogTable(String tableName, String schemaName) {
        super(tableName,schemaName,CREATE_STRING);
    }
}
