'use strict';

angular
	.module('spliceWebApp', [
		'ngCookies',
		'ngResource',
		'ngSanitize',
		'ngRoute',
		'spliceWebServices',
		'spliceWebControllers',
		'spliceWebDirectives'
	])
	.config(function ($routeProvider) {  // Configure routes
		$routeProvider
			.when('/traced-statements', {
				templateUrl: 'views/traced-statement-list.html',
				controller: 'TracedStatementListCtrl'
			})
			.when('/traced-statements/:statementId', {
				templateUrl: 'views/traced-statement-detail.html',
				controller: 'TracedStatementDetailCtrl',
				// Do not render until the traced statement has been returned from the Splice REST services.
				// In the future, we can start rendering the tree and refresh it as the REST services return.
				resolve: {
					tracedStatement: ['TracedStatementDetailService', '$route', function(TracedStatementDetailService, $route) {
						return TracedStatementDetailService.get({statementId: $route.current.params.statementId}).$promise;
					}],
					tracedStatementSQL: ['TracedStatementSQLService', '$route', function(TracedStatementSQLService, $route) {
						return TracedStatementSQLService.query({statementId: $route.current.params.statementId}).$promise;
					}]
				}
			})
			.when('/yeoman', {
				templateUrl: 'views/main.html',
				controller: 'MainCtrl'
			})
			.otherwise({
				redirectTo: '/traced-statements'
			});
	});

// Setup dependency injection of D3 to keep from loading the library into the global scope.
// TODO: The dependency injection of d3.js is not working and the benefit is minimal at best.
//angular.module('d3', []);
angular.module('spliceWebControllers', []);
//angular.module('spliceWebDirectives', ['d3']);
angular.module('spliceWebDirectives', []);
