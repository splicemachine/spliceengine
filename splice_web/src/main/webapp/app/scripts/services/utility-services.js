'use strict';

var spliceAdminServices = angular.module('spliceAdminServices');

spliceAdminServices.factory('formatNanoTimeService', function() {
	return {
		formatRawNanoTime: function(rawNanoTime) {
			return Math.round(rawNanoTime/1000000) + 'ms';
		}
	}
});