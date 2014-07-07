(function () {
	'use strict';

	angular.module('spliceWebDirectives')
		.directive('spliceOpsTree', [/*'d3',*/ function(/*d3*/) {  // TODO: Dependency injection is not working and suspect the value that it provides.
			return {
				restrict: 'EA',
				scope: {
					data: "=",
					label: "@",
					onClick: "&"
				},
				link: function(scope, iElement, iAttrs) {
					var svg = d3.select(iElement[0])
						.append("svg:svg")
						.attr("width", "100%");

					// Browser onresize event: On window resize, re-render d3 canvas.
					window.onresize = function() {
						return scope.$apply();
					};

					// Watch for resize event.
					scope.$watch(
						function(){
							return angular.element(window)[0].innerWidth;
						},
						function(){
							return scope.render(scope.data);
						}
					);

					// TODO: This is problematic code that causes many executions of the watcher.
					// Watch for data changes and re-render.
//					scope.$watch(
//						'data',
//						function(newVals, oldVals) {
//							console.log("newVals = ", newVals, "oldVals = ", oldVals);
//							return scope.render(newVals);
//						},
//						true
//					);

					// Define render function.
					scope.render = function(data){
						// remove all previous items before render
						svg.selectAll("*").remove();

						// Create a svg canvas.
						var vis = svg
//						var vis = d3.select("#viz").append("svg:svg")
//							.attr("width", 400)
							.attr("height", 300)
							.append("svg:g")
							.attr("transform", "translate(100, 40)"); // Shift everything to the right.

						// Create a tree "canvas".
						var tree = d3.layout.tree()
							.size([300,150]);

						var diagonal = d3.svg.diagonal()
							// Change x and y (for the left to right tree).
							.projection(function(d) { return [d.x, d.y]; });

						// Preparing the data for the tree layout, convert data into an array of nodes.
						var nodes = tree.nodes(data);
						// Create an array with all the links
						var links = tree.links(nodes);

//						console.log("this", this)
						console.log("data", data)
//						console.log("data.Stringify", JSON.stringify(data, null, 10))
						console.log("nodes", nodes)
						console.log("links", links)

						var link = vis.selectAll("pathlink")
							.data(links)
							.enter().append("svg:path")
							.attr("class", "link")
							.attr("d", diagonal)

						var node = vis.selectAll("g.node")
							.data(nodes)
							.enter().append("svg:g")
							.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; })

						// Add the dot at every node.
						node.append("svg:circle")
							.on("click", function(d, i){return scope.onClick({item: d});})
							.attr("r", 3.5);

						// Place the name attribute left or right depending if children exist.
						node.append("svg:text")
							.attr("dx", function(d) { return d.children ? -8 : 8; })
							.attr("dy", 3)
							.attr("text-anchor", function(d) { return d.children ? "end" : "start"; })
							.text(function(d) { return d[scope.label]; })
					};  // End of render function.
				}
			};
		}]);
}());
