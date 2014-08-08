(function () {
	'use strict';

	angular.module('spliceAdminDirectives')
		.directive('spliceAdminHeader', function() {
			return {
				restrict: 'E',
				templateUrl: 'views/directives/admin-header.html'
			};
		});
}());
