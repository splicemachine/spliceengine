'use strict';

angular
	.module('spliceAdminApp', [
		'ngCookies',
		'ngResource',
		'ngSanitize',
		'ngRoute',
		'spliceAdminServices',
		'spliceAdminControllers',
		'spliceAdminDirectives'
	])
	.config(function ($routeProvider) {  // Configure routes
		$routeProvider
			.when('/traced-statements', {
				templateUrl: 'views/traced-statement-list.html',
				controller: 'tracedStatementListController'
			})
			.when('/traced-statements/:statementId', {
				templateUrl: 'views/traced-statement-detail.html',
				controller: 'tracedStatementDetailController',
				// Do not render until the traced statement has been returned from the Splice REST services.
				// In the future, we can start rendering the tree and refresh it as the REST services return.
				resolve: {
					tracedStatement: ['tracedStatementDetailService', '$route', function(tracedStatementDetailService, $route) {
						return tracedStatementDetailService.get({statementId: $route.current.params.statementId}).$promise;
					}],
					tracedStatementSQL: ['tracedStatementSQLService', '$route', function(tracedStatementSQLService, $route) {
						return tracedStatementSQLService.query({statementId: $route.current.params.statementId}).$promise;
					}]
				}
			})
			.when('/yeoman', {
				templateUrl: 'views/main.html',
				controller: 'mainController'
			})
			.otherwise({
				redirectTo: '/traced-statements'
			});
	});

// Setup dependency injection of D3 to keep from loading the library into the global scope.
// TODO: The dependency injection of d3.js is not working and the benefit is minimal at best.
//angular.module('d3', []);
angular.module('spliceAdminControllers', []);
//angular.module('spliceAdminDirectives', ['d3']);
angular.module('spliceAdminDirectives', []);
