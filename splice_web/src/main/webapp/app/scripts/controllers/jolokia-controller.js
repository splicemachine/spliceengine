'use strict';
angular.module('spliceAdminControllers')
	.controller('jolokiaController', ['$scope', 'jolokiaServices', function ($scope, jolokiaServices) {
		$scope.updateData = function () {
			$scope.memoryData = jolokiaServices.getMemory();
		}
}]);
