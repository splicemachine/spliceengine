(function () {
	'use strict';

	angular.module('spliceAdminControllers')
		.controller('chartsRegionServerStatsD3BarsController', ['$scope', function($scope){

			// Method to update the d3Data with the selected statistics name.
			$scope.updateSelectedStatsName = function(name) {
				$scope.title = name;
				// Plug in the data from the Region Server Stats RESTful service.
				// d3Data is used by the d3-bars directive to render the bar charts.
				$scope.d3Data = [];
				for (var i = 0; i < $scope.regionServerStats.length; i++) {
					$scope.d3Data.push({name: $scope.regionServerStats[i].host, score: $scope.regionServerStats[i][name]});
				}
			}

			$scope.d3OnClick = function (item) {
				alert(item.name);
			};

			// Watch for changes to the selected statistics name and update the d3Data which will cause a re-render of the bar charts.
			$scope.$watch('selectedRegionServerStat.name', function(newValue, oldValue) {
				// Only set the name if the value changed.
				if ( newValue !== oldValue ) {
					$scope.updateSelectedStatsName(newValue);
				}
			});

			// Update the d3Data with the selected statistics name.
			$scope.updateSelectedStatsName($scope.selectedRegionServerStat.name);
		}]);

}());
