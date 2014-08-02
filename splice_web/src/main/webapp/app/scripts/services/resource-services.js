'use strict';

var spliceAdminServices = angular.module('spliceAdminServices');
// Set the URL prefix if you want your client to connect to another Splice REST server.
// You will need to enable CORS on the server (or disable security on your browser for development purposes only!).
//var urlPrefix = '/splice_web';  // Prefix for ancient versions of Jetty, such as those used by HBase.
//var urlPrefix = 'http://ec2-54-226-199-122.compute-1.amazonaws.com:8080';
var urlPrefix = '';

// Return the list of all traced statements stored in Splice.
spliceAdminServices.factory('tracedStatementListService',
	['$resource',
		function($resource){
			return $resource(urlPrefix + '/webresources/sqlresource/query2js?query=' +
				encodeURIComponent('select * from SYS.SYSSTATEMENTHISTORY order by STARTTIMEMS desc {LIMIT 100}'), {}, {
				query: {method:'GET', isArray:true}
			});
		}]);

// Return the traced "explain" plan as a JSON tree for a specific statement.
spliceAdminServices.factory('tracedStatementDetailService',
	['$resource',
		function($resource){
			return $resource(urlPrefix + '/webresources/sqlresource/tracedStatements/:statementId', {}, {
				get: {method:'GET'}
			});
		}]);

// Return the SQL for a specific statement.
spliceAdminServices.factory('tracedStatementSQLService',
	['$resource',
		function($resource){
			return $resource(urlPrefix + '/webresources/sqlresource/query2js?query=' +
				encodeURIComponent('select STATEMENTSQL from SYS.SYSSTATEMENTHISTORY where STATEMENTID=') + ':statementId', {}, {
				query: {method:'GET', isArray:true}
			});
		}]);

// Check whether the last Splice system table has been created for the primary JDBC connection.
// TODO: In the future, cache all nodes in the cluster and check all of them in case the primary connection goes down.
// Once the SYS.SYSPRIMARYKEYS table (CONGLOMERATENUMBER==1168) has been created, Splice is ready to accept connections
// and respond to statement requests.
spliceAdminServices.factory('primaryServerStatusCheckService',
	['$resource',
		function($resource){
			return $resource(urlPrefix + '/webresources/sqlresource/query2js?query=' +
				encodeURIComponent('select CONGLOMERATENUMBER from SYS.SYSCONGLOMERATES where CONGLOMERATENUMBER >= 1168 {LIMIT 1}'), {}, {
				query: {method:'GET', isArray:true}
			});
		}]);

// Check each server in the cluster and test whether each can process a SQL query request.
spliceAdminServices.factory('serversStatusCheckService',
	['$resource',
		function($resource){
			return $resource(urlPrefix + '/webresources/sqlresource/servers/status', {}, {
				query: {method:'GET'}
			});
		}]);

// Return the statistics for all region servers.
spliceAdminServices.factory('getRegionServerStatsService',
	['$resource',
		function($resource){
			return $resource(urlPrefix + '/webresources/sqlresource/query2js?query=' +
				encodeURIComponent('call syscs_util.SYSCS_GET_REGION_SERVER_STATS_INFO()'), {}, {
				query: {method:'GET', isArray:true}
			});
		}]);
