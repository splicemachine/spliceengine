'use strict';

angular
	.module('spliceAdminApp', [
		'ngCookies',
		'ngResource',
		'ngSanitize',
		'ngRoute',
		'nvd3ChartDirectives',
		'angularTreeview',
		'treeControl',
		'ui.bootstrap',
		'ui.tree',
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
			.when('/traced-statements-basic/:statementId', {
				templateUrl: 'views/traced-statement-detail-basic.html',
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
			.when('/traced-statements-d3/:statementId', {
				templateUrl: 'views/traced-statement-detail-d3.html',
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
			.when('/traced-statements-treeview/:statementId', {
				templateUrl: 'views/traced-statement-detail-treeview.html',
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
			.when('/traced-statements-tree-control/:statementId', {
				templateUrl: 'views/traced-statement-detail-tree-control.html',
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
			.when('/traced-statements-ui-tree/:statementId', {
				templateUrl: 'views/traced-statement-detail-ui-tree.html',
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
			.when('/home', {
				redirectTo: '/traced-statements'
			})
			.when('/main', {
				templateUrl: 'views/main.html',
				controller: 'mainController'
			})
			.when('/contact', {
				templateUrl: 'views/contact.html'
			})
			.when('/d3-bars', {
				templateUrl: 'views/d3-bars.html'
			})
			.when('/charts-region-server-stats', {
				templateUrl: 'views/charts-region-server-stats.html',
				controller: 'chartsRegionServerStatsController',
				// Do not render until the region server stats have been returned from the Splice REST services.
				// In the future, we can start rendering the tree and refresh it as the REST services return.
				resolve: {
					regionServerStats: ['getRegionServerStatsService', '$route', function(getRegionServerStatsService, $route) {
						return getRegionServerStatsService.query().$promise;
					}]
				}
			})
			.otherwise({
				redirectTo: '/home'
			});
	});

// Create all Splice Admin modules here since they may be shared across files.
// Setup dependency injection of D3 to keep from loading the library into the global scope.
// TODO: The dependency injection of d3.js is not working and the benefit is minimal at best.
//angular.module('d3', []);
angular.module('spliceAdminServices', ['ngResource']);
angular.module('spliceAdminControllers', []);
//angular.module('spliceAdminDirectives', ['d3']);
angular.module('spliceAdminDirectives', []);
