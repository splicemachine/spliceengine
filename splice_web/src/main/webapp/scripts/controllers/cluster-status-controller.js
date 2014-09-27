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
			function(value, responseHeaders) {  // Success callback
				$scope.primaryServerStatusCheckSuccess = true;
			},
			function(httpResponse) {  // Failure callback
				$scope.primaryServerStatusCheckSuccess = false;
			}
		);
		// Check that every Splice server is able to process SQL requests.
		$scope.serversStatusCheckResponse = serversStatusCheckService.query(
			[],
			function(value, responseHeaders) {  // Success callback
				// Create the message to display when a user mouses over the green thumbs-up icon for the cluster.
				var servers = {
					UP: [],
					DOWN: []
				};
				angular.forEach(value.servers, function(value2, key) {
					if (value2.UP === 'true') {
						this.UP.push(value2.HOSTNAME);
					} else {
						this.DOWN.push(value2.HOSTNAME);
					}
				}, servers);
				$scope.upServersMessage = servers.UP.join(', ');
				$scope.downServersMessage = servers.DOWN.join(', ');
			},
			function(httpResponse) {  // Failure callback
				// No-op
			}
		);
	}]);
