(function () {
	'use strict';

	angular.module('spliceWebControllers')
		.controller('d3BarsCtrl', ['$scope', function($scope){
			$scope.title = "d3BarsCtrl";
			// TODO: Remove the hard coded sample data and hook it up to a RESTful service.
			$scope.d3Data = [
				{name: "Greg", score:98},
				{name: "Ari", score:96},
				{name: "Loser", score: 48}
			];
			$scope.d3OnClick = function(item){
				alert(item.name);
			};
		}]);

}());
