'use strict';

angular.module('spliceAdminControllers')
	// The tracedStatement below is fully loaded since the route provider blocks until the RESTful service returns.
	// In the future, this may change and the tree structure will refresh when the service returns.
	.controller('tracedStatementDetailController',
	['$scope', '$routeParams', 'tracedStatement', 'tracedStatementSQL', 'formatNanoTimeService', function ($scope, $routeParams, tracedStatement, tracedStatementSQL, formatNanoTimeService) {
		$scope.tree = [tracedStatement];
		$scope.tracedStatementSQL = tracedStatementSQL[0].STATEMENTSQL;

		// Function to format raw nanoseconds into a more readable format.
		$scope.formatRawNanoTime = function(rawNanoTime) { return formatNanoTimeService.formatRawNanoTime(rawNanoTime); };

		// Function to return an array of all nodes in a tree.  Used to specify that all nodes should be initially expanded.
		$scope.getAllNodes = function(nodes) {
			var allNodes = [];
			if (nodes != undefined) {
				for (var i = 0; i < nodes.length; i++) {
					var node = nodes[i];
					allNodes.push(node);
					if (node.hasOwnProperty('children')) {
						allNodes.push.apply(allNodes, $scope.getAllNodes(node.children));
					}
				}
			}
			return allNodes;
		}

		$scope.expandedNodes = $scope.getAllNodes($scope.tree);
		$scope.opts = {
			// This is used to disambiguate between operation nodes with the same label.
			// TODO: In the future, this comparison function can use the operationId for uniqueness and a performance gain.
			equality: function(node1, node2) {
				return node1 === node2;
			},
			// This is used to set styles on the HTML elements.
			injectClasses: {
//				"ul": "angular-ui-tree-nodes",
//				"ol": "angular-ui-tree-nodes",
//				"li": "my-angular-ui-tree-handle"
			}
		}
	}]);
