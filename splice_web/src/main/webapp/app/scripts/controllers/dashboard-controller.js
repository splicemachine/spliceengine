'use strict';

angular.module('spliceAdminControllers')
	.controller('dashboardController', ['$scope', '$timeout',
		function($scope, $timeout) {
			$scope.gridsterOptions = {
				margins: [20, 20],
				columns: 12,
				draggable: {
					handle: 'h5'
				},
				resizable: {
					enabled: true,
					handles: 'n, e, s, w, ne, se, sw, nw',
				}
			};

			$scope.dashboards = {
				'2': {
					id: '2',
					name: 'Home',
					widgets: [{
						col: 0,
						row: 0,
						sizeY: 2,
						sizeX: 4,
						name: "Widget 1"
					}, {
						col: 2,
						row: 1,
						sizeY: 2,
						sizeX: 4,
						name: "Widget 2"
					}],

				},
				'1': {
					id: '1',
					name: 'Other',
					widgets: [{
						col: 0,
						row: 0,
						sizeY: 2,
						sizeX: 8,
						name: "Parser"
					}, {
						col: 9,
						row: 0,
						sizeY: 2,
						sizeX: 4,
						name: "Zookeeper"
					},  {
						col: 0,
						row: 2,
						sizeY: 2,
						sizeX: 8,
						name: "Resource Management"
					}, {
						col: 9,
						row: 2,
						sizeY: 2,
						sizeX: 4,
						name: "Timestamp Oracle"
					}, {
						col: 0,
						row: 4,
						sizeY: 5,
						sizeX: 8,
						name: "Hbase",
					}, {
						col: 9,
						row: 4,
						sizeY: 5,
						sizeX: 4,
						name: "Hbase Network",
					}, {
						col: 0,
						row: 11,
						sizeY: 2,
						sizeX: 4,
						name: "Local Filesystem (SSR)"
					}, {
						col: 4,
						row: 11,
						sizeY: 2,
						sizeX: 4,
						name: "Hadoop Filesystem (HDFS)",
					}, {
						col: 9,
						row: 11,
						sizeY: 2,
						sizeX: 4,
						name: "HDFS Network"
					}

					]
				},
			};

			$scope.clear = function() {
				$scope.dashboard.widgets = [];
			};

			$scope.addWidget = function() {
				$scope.dashboard.widgets.push({
					name: "New Widget",
					sizeX: 4,
					sizeY: 2
				});
			};

			$scope.$watch('selectedDashboardId', function(newVal, oldVal) {
				if (newVal !== oldVal) {
					$scope.dashboard = $scope.dashboards[newVal];
				} else {
					$scope.dashboard = $scope.dashboards[1];
				}
			});

			// init dashboard
			$scope.selectedDashboardId = '1';

		}
	])

	.controller('customWidgetController', ['$scope', '$modal',
		function($scope, $modal) {

			$scope.remove = function(widget) {
				$scope.dashboard.widgets.splice($scope.dashboard.widgets.indexOf(widget), 1);
			};

			$scope.openSettings = function(widget) {
				$modal.open({
					scope: $scope,
					templateUrl: 'views/widget-settings.html',
					controller: 'widgetSettingsController',
					resolve: {
						widget: function() {
							return widget;
						}
					}
				});
			};

		}
	])

	.controller('widgetSettingsController', ['$scope', '$timeout', '$rootScope', '$modalInstance', 'widget',
		function($scope, $timeout, $rootScope, $modalInstance, widget) {
			$scope.widget = widget;

			$scope.form = {
				name: widget.name,
				sizeX: widget.sizeX,
				sizeY: widget.sizeY,
				col: widget.col,
				row: widget.row
			};

			$scope.sizeOptions = [{
				id: '1',
				name: '1'
			}, {
				id: '2',
				name: '2'
			}, {
				id: '3',
				name: '3'
			}, {
				id: '4',
				name: '4'
			}];

			$scope.dismiss = function() {
				$modalInstance.dismiss();
			};

			$scope.remove = function() {
				$scope.dashboard.widgets.splice($scope.dashboard.widgets.indexOf(widget), 1);
				$modalInstance.close();
			};

			$scope.submit = function() {
				angular.extend(widget, $scope.form);

				$modalInstance.close(widget);
			};

		}
	])

	// helper code
	.filter('object2Array', function() {
		return function(input) {
			var out = [];
			for (var i= 0; i < input.length; i++) {
				out.push(input[i]);
			}
			return out;
		}
	});
