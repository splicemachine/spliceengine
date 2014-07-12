'use strict';

angular.module('spliceAdminControllers')
	.controller('mainController', ['$scope', 'sysTableCheckService', function ($scope, sysTableCheckService) {
		$scope.sysTableCheckResponse = sysTableCheckService.query(
			[],
			function(value, responseHeaders) {
				$scope.sysTableCheckSuccess = true;
			},
			function(httpResponse) {
				$scope.sysTableCheckSuccess = false;
			}
		);
	}]);
