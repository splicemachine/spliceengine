/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

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