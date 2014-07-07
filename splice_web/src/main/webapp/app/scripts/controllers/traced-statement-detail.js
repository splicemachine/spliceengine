'use strict';

angular.module('spliceWebApp')
	// The tracedStatement below is fully loaded since the route provider blocks until the RESTful service returns.
	// In the future, this may change and the tree structure will refresh when the service returns.
	.controller('TracedStatementDetailCtrl', ['$scope', '$routeParams', 'tracedStatement', function ($scope, $routeParams, tracedStatement) {
		$scope.tree = [tracedStatement];
		console.log("tracedStatement", tracedStatement);
		console.log("tracedStatementStr #1", JSON.stringify(tracedStatement,undefined,4));
		$scope.awesomeThings = [
			'HTML5 Boilerplate',
			'AngularJS',
			'Karma'
		];
	}]);
