'use strict';

angular.module('spliceAdminControllers')
	// The tracedStatement below is fully loaded since the route provider blocks until the RESTful service returns.
	// In the future, this may change and the tree structure will refresh when the service returns.
	.controller('tracedStatementDetailController',
	['$scope', '$routeParams', 'tracedStatement', 'tracedStatementSQL', 'formatNanoTimeService', function ($scope, $routeParams, tracedStatement, tracedStatementSQL, formatNanoTimeService) {
		$scope.tree = [tracedStatement];
		$scope.tracedStatementSQL = tracedStatementSQL[0].STATEMENTSQL;
		$scope.formatRawNanoTime = function(rawNanoTime) { return formatNanoTimeService.formatRawNanoTime(rawNanoTime); };
	}]);
