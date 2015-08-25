$(document).ready(function () {
    var fileControl = new sm.FileControl();
    var graphControl = new sm.GraphControl();

    fileControl.setFileContentCallback(function (graphModel) {
        graphControl.setNewModel(graphModel);
    });
});
