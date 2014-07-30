'use strict';

angular.module('spliceAdminControllers')
	.controller('clusterStatusController',
	['$scope', 'primaryServerStatusCheckService', 'serversStatusCheckService', function ($scope, primaryServerStatusCheckService, serversStatusCheckService) {
		// TODO: Merge these two "check" services into a single service to streamline and simplify.
		// Check if the primary Splice server is up.
		// TODO: Remove the single point of failure where the admin is dependent on a single Splice server.
		// The admin server (or client) could cache JDBC URLs to all Splice servers.
		$scope.primaryServerStatusCheckResponse = primaryServerStatusCheckService.query(
			[],
			function(value, responseHeaders) {
				$scope.primaryServerStatusCheckSuccess = true;
			},
			function(httpResponse) {
				$scope.primaryServerStatusCheckSuccess = false;
			}
		);
		// Check that every Splice server is able to process SQL requests.
		$scope.serversStatusCheckResponse = serversStatusCheckService.query();
	}]);
