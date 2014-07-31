'use strict';

angular.module('spliceAdminControllers')
	.controller('mainController', ['$scope', function ($scope) {
		// Set the active tab to be highlighted in the top nav bar.
		$scope.homeTab = 'active';
	}]);
