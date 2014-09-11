'use strict';

angular.module('spliceAdminControllers')
	// The regionServerStats below is fully loaded since the route provider blocks until the RESTful service returns.
	// In the future, this may change and the regionServerStats structure will refresh when the service returns.
	.controller('chartsRegionServerStatsController',
	['$scope', '$routeParams', 'regionServerStats', function ($scope, $routeParams, regionServerStats) {
		$scope.regionServerStats = regionServerStats;
	}]);
