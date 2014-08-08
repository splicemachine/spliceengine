'use strict';

angular.module('spliceAdminControllers')
	.controller('contactController', ['$scope', function ($scope) {
		// Set the active tab to be highlighted in the top nav bar.
		$scope.contactTab = 'active';
	}]);
