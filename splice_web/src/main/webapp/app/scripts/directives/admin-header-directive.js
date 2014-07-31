(function () {
	'use strict';

	angular.module('spliceAdminDirectives')
		.directive('adminHeader', function() {
			return {
				restrict: 'E',
				templateUrl: 'views/directives/admin-header.html'
			};
		});
}());
