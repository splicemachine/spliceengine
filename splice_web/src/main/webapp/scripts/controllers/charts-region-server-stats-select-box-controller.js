angular.module('spliceAdminControllers')
	.controller('chartsRegionServerStatsSelectBoxController', ['$scope', function($scope) {
		$scope.regionServerStatsSelectList = [
			{name: 'readRequestsCount', group: 'I/O'},
			{name: 'fsReadLatencyAvgTime', group: 'I/O'},
			{name: 'writeRequestsCount', group: 'I/O'},
			{name: 'fsWriteLatencyAvgTime', group: 'I/O'},
			{name: 'requests', group: 'System'},
			{name: 'regions', group: 'System'},
			{name: 'compactionQueueSize', group: 'System'},
			{name: 'flushQueueSize', group: 'System'}
		];
		$scope.selectedRegionServerStat = $scope.regionServerStatsSelectList[4];  // Total Requests
	}]);
