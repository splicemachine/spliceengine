'use strict';

angular.module('spliceAdminServices')
	.service('jolokiaServices', function() {
		var j4p = new Jolokia("http://jolokia.org/jolokia");
		var context = cubism.context()
							    .serverDelay(0)
							    .clientDelay(0)
							    .step(1000)
								.size(594);
					
		var jolokia = context.jolokia(j4p);

		var memoryData = function (jolokia) {
			jolokia.metric(function (resp1, resp2) {
                //console.log(Number(resp1.value) / Number(resp2.value));
		        return Number(resp1.value) / Number(resp2.value);
		    },
		    {type:"read", mbean:"java.lang:type=Memory", attribute:"HeapMemoryUsage", path:"used"},
		    {type:"read", mbean:"java.lang:type=Memory", attribute:"HeapMemoryUsage", path:"max"}, "Heap-Memory"
		)};
    	j4p.start(1000);

			return {
				getMemory: function (){
					return memoryData(jolokia);
				}
			};
});