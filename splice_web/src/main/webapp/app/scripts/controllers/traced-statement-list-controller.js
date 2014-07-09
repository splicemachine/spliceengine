'use strict';

angular.module('spliceAdminApp')
	.controller('tracedStatementListController', ['$scope', 'tracedStatementListService', function ($scope, tracedStatementListService) {
		$scope.tracedStatements = tracedStatementListService.query();
	}]);
