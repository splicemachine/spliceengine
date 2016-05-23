var sm = sm || {};

/**
 *
 * @constructor
 */
sm.FileControl = function () {
    this.checkApiSupport();

    this.fileInput = $("#file-input");
    this.fileInput.change(jQuery.proxy(this.loadFile, this));
};


sm.FileControl.prototype.checkApiSupport = function () {
    if (typeof window.FileReader !== 'function') {
        alert("The file API isn't supported on this browser yet.");
    }
};

sm.FileControl.prototype.loadFile = function () {
    var file = this.fileInput.get(0).files[0];
    var fr = new FileReader();
    fr.onload = jQuery.proxy(this.receivedText, this);
    fr.readAsText(file);
};

sm.FileControl.prototype.receivedText = function (e) {
    var lines = e.target.result;
    var graphContainer = JSON.parse(lines);
    this.fileContentCallback.call(this, graphContainer);
};

sm.FileControl.prototype.setFileContentCallback = function (callback) {
    this.fileContentCallback = callback;
};