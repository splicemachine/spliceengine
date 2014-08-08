'use strict';

angular.module('spliceAdminControllers')
	.controller('tracedStatementListController', ['$scope', 'tracedStatementListService', function ($scope, tracedStatementListService) {
		// Set the active tab to be highlighted in the top nav bar.
		$scope.homeTab = 'active';

		$scope.tracedStatements = tracedStatementListService.query();
	}]);
