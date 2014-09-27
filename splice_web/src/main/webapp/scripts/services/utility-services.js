'use strict';

var spliceAdminServices = angular.module('spliceAdminServices');

spliceAdminServices.factory('formatUnitsService', function() {
	return {
		// Format the raw time into the smallest whole number units.
		formatRawNanoTime: function(rawNanoTime) {
			var tempTime = rawNanoTime;
			if (tempTime < 1000) {  // nanoseconds
				return tempTime + 'ns';
			} else {
				tempTime = Math.round(rawNanoTime / 1000);
				if (tempTime < 1000) {  // microseconds
					return tempTime + "\u00B5s";
				} else {
					tempTime = Math.round(rawNanoTime / (1000*1000));
					if (tempTime < 1000) {  // milliseconds
						return tempTime + 'ms';
					} else {
						tempTime = Math.round(rawNanoTime / (1000*1000*1000));
						if (tempTime < 60) {  // seconds
							return tempTime + 's';
						} else {
							tempTime = Math.round(rawNanoTime / (1000*1000*1000*60));
							if (tempTime < 60) {  // minutes
								return tempTime + 'm';
							} else {
								tempTime = Math.round(rawNanoTime / (1000*1000*1000*60*60));
								if (tempTime < 24) {  // hours
									return tempTime + 'h';
								} else {
									tempTime = Math.round(rawNanoTime / (1000*1000*1000*60*60*24));  // days
									return tempTime + 'd';
								}
							}
						}
					}
				}
			}
		}
	}
});