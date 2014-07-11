'use strict';

angular.module('spliceAdminControllers')
	.controller('tracedStatementListController', ['$scope', 'tracedStatementListService', function ($scope, tracedStatementListService) {
		$scope.tracedStatements = tracedStatementListService.query();
	}]);
