'use strict';

var spliceWebServices = angular.module('spliceWebServices', ['ngResource']);

spliceWebServices.factory('TracedStatementListService',
	['$resource',
		function($resource){
			return $resource('/splice_web/webresources/myresource/sql2js?query=select+*+from+XPLAIN.SYSXPLAIN_STATEMENTHISTORY', {}, {
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
