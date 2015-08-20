var sm = sm || {};


/**
 *
 * @constructor
 */
sm.GraphControl = function () {

    /* the vis.js object */
    this.network = null;

    /* the graph we display */
    this.model = {nodes: [], edges: []};

    // -  - - - - - - - - - - - - - - - Controls

    this.paramLayoutSelect = $("#layout-param-select");
    this.paramLayoutSelect.change(jQuery.proxy(this.paramChanged, this));

    this.paramDirectionSelect = $("#direction-param-select");
    this.paramDirectionSelect.change(jQuery.proxy(this.paramChanged, this));

    this.paramShapeSelect = $("#node-shape-param-select");
    this.paramShapeSelect.change(jQuery.proxy(this.paramChanged, this));

    this.hierarchicalEnabledSelect = $("#hierarchical-enabled-select");
    this.hierarchicalEnabledSelect.change(jQuery.proxy(this.paramChanged, this));

    this.hierarchicalLevelInput = $("#hierarchical-level-separation-input");
    this.hierarchicalLevelInput.change(jQuery.proxy(this.paramChanged, this));

    // -  - - - - - - - - - - - - - - - Control Values DEFAULTS

    this.paramLayout = "directed";
    this.paramDirection = "DU";
    this.paramNodeShape = "box";
    this.paramHierarchicalEnabled = true;
    this.hierarchicalLevel = 125;

};

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

sm.GraphControl.prototype.paramChanged = function (event) {
    this.paramLayout = this.paramLayoutSelect.val();
    this.paramDirection = this.paramDirectionSelect.val();
    this.paramNodeShape = this.paramShapeSelect.val();
    this.paramHierarchicalEnabled = this.hierarchicalEnabledSelect.val() === "true";
    this.hierarchicalLevel = this.hierarchicalLevelInput.val();
    this.draw();
};

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

sm.GraphControl.prototype.setNewModel = function (model) {
    this.model = model;
    this.draw();
};


// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


sm.GraphControl.prototype.destroy = function destroy() {
    if (this.network !== null) {
        this.network.destroy();
        this.network = null;
    }
};


sm.GraphControl.prototype.draw = function () {
    this.destroy();

    var options = {
        configure: {
            enabled: false,
            filter: true,
            showButton: true
        },
        nodes: {
            shape: this.paramNodeShape,
            shadow: true,
            font: {
                size: 16
            }
        },
        edges: {
            color: 'gray',
            smooth: true,
            length: 2,
            physics: true

        },
        physics: {
            barnesHut: {
                springLength: 15,
                gravitationalConstant: -30000
            },
            stabilization: {
                iterations: 2500
            }
        },
        groups: {
            update: {
                font: {
                    color: "red",
                    size: 30
                }
            },
            subquery: {
                font: {
                    color: "blue",
                    size: 30
                }
            }
        },
        layout: {
            hierarchical: {
                direction: this.paramDirection,
                sortMethod: this.paramLayout,
                levelSeparation: this.hierarchicalLevel,
                enabled: this.paramHierarchicalEnabled

            }
        },
        interaction: {
            dragNodes: true,
            dragView: true,
            hideEdgesOnDrag: false,
            hideNodesOnDrag: false,
            hover: false,
            hoverConnectedEdges: true,
            keyboard: {
                enabled: false,
                speed: {x: 10, y: 10, zoom: 0.02},
                bindToWindow: true
            },
            multiselect: false,
            navigationButtons: false,
            selectable: true,
            selectConnectedEdges: true,
            tooltipDelay: 300,
            zoomView: true
        },
        manipulation: {
            enabled: true,
            initiallyActive: true,
            addNode: true,
            addEdge: true,
            editNode: undefined,
            editEdge: true,
            deleteNode: true,
            deleteEdge: true,
            controlNodeStyle: {}
        }
    };

    var container = document.getElementById('graph-container-div');

    this.network = new vis.Network(container, this.model, options);
};
