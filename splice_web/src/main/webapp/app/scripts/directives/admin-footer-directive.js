(function () {
	'use strict';

	angular.module('spliceAdminDirectives')
		.directive('spliceAdminFooter', function() {
			return {
				restrict: 'E',
				templateUrl: 'views/directives/admin-footer.html'
			};
		});
}());
