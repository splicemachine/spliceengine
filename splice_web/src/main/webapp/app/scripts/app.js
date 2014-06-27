'use strict';

angular
  .module('spliceWebApp', [
    'ngCookies',
    'ngResource',
    'ngSanitize',
    'ngRoute',
    'spliceWebServices'
  ])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/traced-statements', {
        templateUrl: 'views/traced-statement-list.html',
        controller: 'TracedStatementListCtrl'
      })
      .when('/traced-statements/:statementId', {
        templateUrl: 'views/traced-statement-detail.html',
        controller: 'TracedStatementDetailCtrl'
      })
      .when('/yeoman', {
        templateUrl: 'views/main.html',
        controller: 'MainCtrl'
      })
      .otherwise({
        redirectTo: '/traced-statements'
      });
  });
