'use strict';

var spliceAdminServices = angular.module('spliceAdminServices');

// Return the list of all traced statements stored in Splice.
spliceAdminServices.factory('tracedStatementListService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=' +
				encodeURIComponent('select * from SYS.SYSSTATEMENTHISTORY order by STARTTIMEMS desc'), {}, {
				query: {method:'GET', isArray:true}
			});
		}]);

// Return the traced "explain" plan as a JSON tree for a specific statement.
spliceAdminServices.factory('tracedStatementDetailService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/tracedStatements/:statementId', {}, {
				get: {method:'GET'}
			});
		}]);

// Return the SQL for a specific statement.
spliceAdminServices.factory('tracedStatementSQLService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=' +
				encodeURIComponent('select STATEMENTSQL from SYS.SYSSTATEMENTHISTORY where STATEMENTID=') + ':statementId', {}, {
				query: {method:'GET', isArray:true}
			});
		}]);

// Check whether the last Splice system table has been created.  Once the SYS.SYSPRIMARYKEYS table (CONGLOMERATENUMBER==1168)
// has been created, Splice is ready to accept connections and respond to statement requests.
spliceAdminServices.factory('sysTableCheckService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=' +
				encodeURIComponent('select CONGLOMERATENUMBER from SYS.SYSCONGLOMERATES where CONGLOMERATENUMBER >= 1168 {LIMIT 1}'), {}, {
				query: {method:'GET', isArray:true}
			});
		}]);
