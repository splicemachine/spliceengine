(function () {
	'use strict';

	angular.module('spliceWebControllers')
		.controller('spliceOperationsTreeCtrl', ['$scope', function($scope){
			$scope.title = "spliceOperationsTreeCtrl";
			// TODO: Remove hard coded sample data.
//			$scope.treeData =
//			{"name" : "A", "info" : "tst", "children" :
//				[{"name" : "A1" },
//					{"name" : "A2" },
//					{"name" : "A3", "children":
//						[{"name" : "A31", "children" :
//							[{"name" : "A311" },
//								{"name" : "A312" }
//							]}] }
//				]};

			$scope.d3OnClick = function(item){
				alert(item.operationType);
			};
		}]);

}());
