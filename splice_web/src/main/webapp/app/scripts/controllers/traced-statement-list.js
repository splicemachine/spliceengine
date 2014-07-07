'use strict';

angular.module('spliceWebApp')
	.controller('TracedStatementListCtrl', ['$scope', 'TracedStatementListService', function ($scope, TracedStatementListService) {
		$scope.tracedStatements = TracedStatementListService.query();
		$scope.awesomeThings = [
			'HTML5 Boilerplate',
			'AngularJS',
			'Karma'
		];
	}]);
