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

    this.hierarchicalEnabledCheckbox = $("#hierarchical-enabled-checkbox");
    this.hierarchicalLevelInput = $("#hierarchical-level-separation-input");
    this.paramLayoutSelect = $("#layout-param-select");
    this.paramDirectionSelect = $("#direction-param-select");
    this.paramShapeSelect = $("#node-shape-param-select");
    this.nodeNameCheckbox = $("#short-node-names-checkbox");

    var changedParamFuncRef = $.proxy(this.paramChanged, this);

    this.hierarchicalEnabledCheckbox.change(changedParamFuncRef);
    this.hierarchicalLevelInput.change(changedParamFuncRef);
    this.paramLayoutSelect.change(changedParamFuncRef);
    this.paramDirectionSelect.change(changedParamFuncRef);
    this.paramShapeSelect.change(changedParamFuncRef);
    this.nodeNameCheckbox.change(changedParamFuncRef);

    // -  - - - - - - - - - - - - - - - Control Values DEFAULTS

    this.paramLayout = "directed";
    this.paramDirection = "DU";
    this.paramNodeShape = "box";
    this.paramHierarchicalEnabled = true;
    this.hierarchicalLevel = 125;
    this.shortNodeNames = false;
};

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

sm.GraphControl.prototype.paramChanged = function (event) {
    this.paramLayout = this.paramLayoutSelect.val();
    this.paramDirection = this.paramDirectionSelect.val();
    this.paramNodeShape = this.paramShapeSelect.val();
    this.paramHierarchicalEnabled = this.hierarchicalEnabledCheckbox.is(":checked");
    this.hierarchicalLevel = this.hierarchicalLevelInput.val();
    this.shortNodeNames = this.nodeNameCheckbox.is(":checked");
    this.draw();
};

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

sm.GraphControl.prototype.setNewModel = function (model) {
    this.model = model;
    this.saveOriginalNodeNames();

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
    this.transformNodeNames();

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
                levelSeparation: parseInt(this.hierarchicalLevel),
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
            editEdge: true,
            deleteNode: true,
            deleteEdge: true,
            controlNodeStyle: {}
        }
    };

    var container = document.getElementById('graph-container-div');

    this.network = new vis.Network(container, this.model, options);

    // add click listener
    this.network.on("selectNode", $.proxy(this.handleNodeClick, this));
};


/**
 * If short names are enabled then display the full node name when the node is clicked.
 */
sm.GraphControl.prototype.handleNodeClick = function (event) {
    if (this.shortNodeNames) {
        var selectedNodeId = event.nodes[0];
        console.log("selectedNodeId=" + selectedNodeId);
        $.each(this.model.nodes, function (index, node) {
            console.log("node.id=" + node.id);
            if (node.id == selectedNodeId) {
                alert("node: " + node.originalLabel);
            }
        });
    }
};

sm.GraphControl.prototype.saveOriginalNodeNames = function () {
    $.each(this.model.nodes, function (index, node) {
        node.label = node.label.replace(/Node$/g, '');
        node.originalLabel = node.label;
    });
};

sm.GraphControl.prototype.transformNodeNames = function () {
    var shortNodeNames = this.shortNodeNames;
    $.each(this.model.nodes, function (index, node) {
        if (shortNodeNames) {
            if (node.label.length > 3) {
                node.label = node.label.replace(/[^A-Z]/g, '');
            }
        } else {
            node.label = node.originalLabel;
        }
    });
};