/*
angular.module('spliceAdminControllers')
.directive('cubismDirective', ['jolokiaController', function (jolokiaController) {
  // constants
  var colorsRed = ["#FDBE85", "#FEEDDE", "#FD8D3C", "#E6550D", "#A63603", "#FDBE85", "#FEEDDE", "#FD8D3C", "#E6550D", "#A63603" ],
    colorsGreen = [ "#E5F5F9", "#99D8C9", "#2CA25F", "#E5F5F9", "#99D8C9", "#2CA25F"],
    colorsBlue = [ "#ECE7F2", "#A6BDDB", "#2B8CBE", "#ECE7F2", "#A6BDDB", "#2B8CBE"];
  
  return {
    restrict: 'E',
    scope: {
      val: '='
    },
    link: function (scope, element, attrs) {

      var vis = d3.select(element[0]);
      console.log("testing");
      scope.$watch('val', function (newVal, oldVal) {

        vis.append("div")
            .attr("class", "axis")
            .call(context.axis().orient("top"));

        // clear the elements inside of the directive
        vis.selectAll('*').remove();
        console.log("in betwwn");
        // if 'val' is undefined, exit
        if (!newVal) {
          return;
        }

        vis.selectAll(".horizon")
            .data([memory])
            .enter().append("div")
            .attr("class", "horizon")
            .call(
            context.horizon()
                .colors(colorsRed)
                .format(d3.format(".4p"))
        );
      });
    }
  };
}]);
*/