'use strict';

var spliceAdminServices = angular.module('spliceAdminServices', ['ngResource']);

spliceAdminServices.factory('tracedStatementListService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=' +
				encodeURIComponent('select * from SYS.SYSSTATEMENTHISTORY order by STARTTIMEMS desc'), {}, {
				query: {method:'GET', isArray:true}
			});
		}]);

spliceAdminServices.factory('tracedStatementDetailService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/tracedStatements/:statementId', {}, {
				get: {method:'GET'}
			});
		}]);

spliceAdminServices.factory('tracedStatementSQLService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=' +
				encodeURIComponent('select STATEMENTSQL from SYS.SYSSTATEMENTHISTORY where STATEMENTID=') + ':statementId', {}, {
				query: {method:'GET', isArray:true}
			});
		}]);
