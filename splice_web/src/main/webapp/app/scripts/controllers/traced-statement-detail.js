'use strict';

angular.module('spliceWebApp')
.controller('TracedStatementDetailCtrl', ['$scope', '$routeParams', 'TracedStatementDetailService', function ($scope, $routeParams, TracedStatementDetailService) {
	var str = TracedStatementDetailService.get({statementId: $routeParams.statementId});
//	$scope.tracedStatement = str;
	$scope.tree = [str];
//	$scope.tracedStatementString = JSON.stringify($scope.tracedStatement, null, 2);
	$scope.awesomeThings = [
	                        'HTML5 Boilerplate',
	                        'AngularJS',
	                        'Karma'
	                        ];
//	$scope.delete = function(data) {
//		data.children = [];
//	};
//	$scope.add = function(data) {
//		var post = data.children.length + 1;
//		var newName = data.name + '-' + post;
//		data.children.push({name: newName,children: []});
//	};
//	$scope.tree = [{name: "Node", children: []}];
}]);
