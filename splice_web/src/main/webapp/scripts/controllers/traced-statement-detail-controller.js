'use strict';

angular.module('spliceAdminControllers')
	// The tracedStatement below is fully loaded since the route provider blocks until the RESTful service returns.
	// In the future, this may change and the tree structure will refresh when the service returns.
	.controller('tracedStatementDetailController',
	['$scope', '$routeParams', 'tracedStatement', 'tracedStatementSQL', 'formatUnitsService', function ($scope, $routeParams, tracedStatement, tracedStatementSQL, formatUnitsService) {
		// Set the active tab to be highlighted in the top nav bar.
		$scope.homeTab = 'active';

		$scope.tree = [tracedStatement];
		$scope.tracedStatementSQL = tracedStatementSQL[0].STATEMENTSQL;

		// Function to format raw nanoseconds into a more readable format.
		$scope.formatRawNanoTime = function(rawNanoTime) {
			return formatUnitsService.formatRawNanoTime(rawNanoTime);
		};

		// Function to expand an operational node with additional information required by the view.
		$scope.expandNode = function(node) {
			if (node.hasOwnProperty('region')) {
				if (node.region.length > 10) {
					var parts = node.region.split(',');
					if (parts.length > 0) {
						if (isNaN(parts[0])) {
							node.regionShort = node.region;
						} else {
							node.regionShort = parts[0] + '...';
						}
					}
				} else {
					node.regionShort = node.region;
				}
			}
		}

		// Function to return an array of all nodes in a tree.  Used to specify that all nodes should be initially expanded.
		$scope.getAllNodes = function(nodes, expandNodeFunc) {
			var allNodes = [];
			if (nodes != undefined) {
				for (var i = 0; i < nodes.length; i++) {
					var node = nodes[i];
					expandNodeFunc(node);
					allNodes.push(node);
					if (node.hasOwnProperty('children')) {
						allNodes.push.apply(allNodes, $scope.getAllNodes(node.children, expandNodeFunc));
					}
				}
			}
			return allNodes;
		}

		// List of nodes that should be initially expanded when the view is rendered.
		$scope.expandedNodes = $scope.getAllNodes($scope.tree, $scope.expandNode);

		// Options to pass to the angular-tree-control.
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
