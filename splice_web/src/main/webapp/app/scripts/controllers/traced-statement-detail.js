'use strict';

angular.module('spliceWebApp')
  .controller('TracedStatementDetailCtrl', ['$scope', 'TracedStatementDetailService', function ($scope, TracedStatementDetailService) {
	  var str = TracedStatementDetailService.get({statementId: $routeParams.statementId});
	  $scope.tracedStatement = str;
	  $scope.tracedStatementString = JSON.stringify(str, undefined, 2);
	  $scope.awesomeThings = [
      'HTML5 Boilerplate',
      'AngularJS',
      'Karma'
    ];
  }]);
