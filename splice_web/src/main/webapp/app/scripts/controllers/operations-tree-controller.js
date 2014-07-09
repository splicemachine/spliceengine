(function () {
	'use strict';

	angular.module('spliceAdminControllers')
		.controller('operationsTreeController', ['$scope', function($scope){
			$scope.title = "operationsTreeController";
			$scope.d3OnClick = function(item){
				$scope.clickedOperation = item;
				$scope.$digest();
			};
		}]);

}());
