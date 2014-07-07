(function () {
	'use strict';

	angular.module('spliceWebControllers')
		.controller('spliceOperationsTreeCtrl', ['$scope', function($scope){
			$scope.title = "spliceOperationsTreeCtrl";
			$scope.d3OnClick = function(item){
				alert(item.operationType);
			};
		}]);

}());
