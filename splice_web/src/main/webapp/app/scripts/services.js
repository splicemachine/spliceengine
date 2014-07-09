'use strict';

var spliceWebServices = angular.module('spliceWebServices', ['ngResource']);

spliceWebServices.factory('TracedStatementListService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=' +
				encodeURIComponent('select * from SYS.SYSSTATEMENTHISTORY'), {}, {
				query: {method:'GET', isArray:true}
			});
		}]);

spliceWebServices.factory('TracedStatementDetailService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/tracedStatements/:statementId', {}, {
				get: {method:'GET'}
			});
		}]);

spliceWebServices.factory('TracedStatementSQLService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=' +
				encodeURIComponent('select STATEMENTSQL from SYS.SYSSTATEMENTHISTORY WHERE STATEMENTID=') + ':statementId', {}, {
				query: {method:'GET', isArray:true}
			});
		}]);
