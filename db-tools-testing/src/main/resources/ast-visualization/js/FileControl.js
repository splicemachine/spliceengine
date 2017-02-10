/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either 
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine. 
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to 
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
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