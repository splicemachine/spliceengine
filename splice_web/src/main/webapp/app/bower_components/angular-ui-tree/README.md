Angular UI Tree
======================

[![Build Status](https://travis-ci.org/JimLiu/angular-ui-tree.png?branch=master)](https://travis-ci.org/JimLiu/angular-ui-tree)

Angular UI Tree is an AngularJS UI component that can sort nested lists, provides drag & drop support and doesn't depend on jQuery. If you are a user who uses `angular-nestedSortable`, this is [How to migrate From v1.x to v2.0](https://github.com/JimLiu/angular-ui-tree/wiki/Migrate-From-v1.x-to-v2.0).


## Features

- Uses the native AngularJS scope for data binding
- Sorted and move items through the entire tree
- Prevent elements from accepting child nodes

## Supported browsers

The Angular UI Tree is tested with the following browsers:

- Chrome (stable)
- Firefox
- IE 8, 9 and 10

For IE8 support, make sure you do the following:

- include an [ES5 shim](https://github.com/es-shims/es5-shim)
- make your [AngularJS application compatible with Internet Explorer](http://docs.angularjs.org/guide/ie)
- use [jQuery 1.x](http://jquery.com/browser-support/)

## Demo
Watch the Tree component in action on the [demo page](http://jimliu.github.io/angular-ui-tree/).

## Requirements

- Angularjs

## Usage

### Download
- Using [bower](http://bower.io/) to install it. `bower install angular-ui-tree`
- [Download](https://github.com/JimLiu/angular-ui-tree/archive/master.zip) from github.

### Load CSS
Load the css file: `angular-ui-tree.min.css` in your application:
```html
<link rel="stylesheet" href="bower_components/angular-ui-tree/dist/angular-ui-tree.min.css">
```


### Load Script
Load the script file: `angular-ui-tree.js` or `angular-ui-tree.min.js` in your application:

```html
<script type="text/javascript" src="bower_components/angular-ui-tree/dist/angular-ui-tree.js"></script>
```

### Code
Add the sortable module as a dependency to your application module:

```js
var myAppModule = angular.module('MyApp', ['ui.tree'])
```

Injecting `ui.tree`, `ui-tree-nodes`, `ui-tree-node`, `ui-tree-handle` to your html.

#### HTML View or Templates
```html
<div ui-tree>
  <ol ui-tree-nodes="" ng-model="list">
    <li ng-repeat="item in list" ui-tree-node>
      <div ui-tree-handle>
        {{item.title}}
      </div>
      <ol ui-tree-nodes="" ng-model="item.items">
        <li ng-repeat="subItem in item.items" ui-tree-node>
          <div ui-tree-handle>
            {{subItem.title}}
          </div>
        </li>
      </ol>
    </li>
  </ol>
</div>
```
**Developing Notes:**
- Adding `ui-tree` to your root element of the tree.
- Adding `ui-tree-nodes` to the elements which contain the nodes. `ng-model` is required, and it should be an array, so that the directive knows which model to bind and update.
- Adding `ui-tree-node` to your node element, it always follows the `ng-repeat` attribute.
- Adding `ui-tree-handle` to the element used to drag the object.
- All `ui-tree`, `ui-tree-nodes`, `ng-model`, `ui-tree-node` are necessary. And they can be nested.
- If you don't add a `ui-tree-handle` for a node, the entire node can be dragged.

#### Unlimited nesting HTML View or Templates Example

```html
<!-- Nested node template -->
<script type="text/ng-template" id="nodes_renderer.html">
  <div ui-tree-handle>
    {{node.title}}
  </div>
  <ol ui-tree-nodes="" ng-model="node.nodes">
    <li ng-repeat="node in node.nodes" ui-tree-node ng-include="'nodes_renderer.html'">
    </li>
  </ol>
</script>
<div ui-tree>
  <ol ui-tree-nodes="" ng-model="data" id="tree-root">
    <li ng-repeat="node in data" ui-tree-node ng-include="'nodes_renderer.html'"></li>
  </ol>
</div>
```

## Structure of angular-ui-tree

    ui-tree                             --> Root of tree
      ui-tree-nodes                     --> Container of nodes
        ui-tree-node                    --> One of the node of a tree
          ui-tree-handle                --> Handle
          ui-tree-nodes                 --> Container of child-nodes
            ui-tree-node                --> Child node
              ui-tree-handle            --> Handle
            ui-tree-node                --> Child node
        ui-tree-node                    --> Another node
          ui-tree-handle                --> Handle

## Migrate From v1.x to v2.0
[Migrate From v1.x to v2.0](https://github.com/JimLiu/angular-ui-tree/wiki/Migrate-From-v1.x-to-v2.0)

## API

### ui-tree
`ui-tree` is the root scope for a tree

#### Attributes
##### data-drag-enabled
Turn on dragging and dropping of nodes.
- `true` (default): allow drag and drop
- `false`: turn off drag and drop

##### data-max-depth
Number of levels a nodes can be nested (default 0). 0 means no limit.
**Note**
If you write your own [$callbacks.accept](#accept) method, you have to check `data-max-depth` by yourself.

##### data-drag-delay
Number of milliseconds a click must be held to start a drag. (default 0)

##### data-empty-place-holder-enabled
If a tree is empty, there will be an empty place hoder which is used to drop node from other trees by default.
- `true` (default): display an empty place holder if the tree is empty
- `false`: do not display an empty place hoder

##### Example 
- turn on/off drag and drop.
- Limit depth to 5
- 500 milliseconds delay
```html
<div ui-tree data-drag-enabled="tree.enabled" data-max-depth="5" data-drag-delay="500">

</div>
```

#### Methods of scope
##### collapseAll()
Collapse all it's child nodes.

##### expandAll()
Expand all it's child nodes.

##### $callbacks (type: Object)
`$callbacks` is a very important property for `angular-ui-tree`. When some special events trigger, the functions in `$callbacks` are called. The callbacks can be passed through the directive.
Example:
```js
myAppModule.controller('MyController', function($scope) {
  $scope.treeOptions = {
    accept: function(sourceNodeScope, destNodesScope, destIndex) {
      return true;
    },
  };
});
```
```html
<div ui-tree="treeOptions">
  <ol ui-tree-nodes ng-model="nodes">
    <li ng-repeat="node in nodes" ui-tree-node>{{node.title}}</li>
  </ol>
</div>
```

#### Methods in $callbacks
##### <a name="accept"></a>accept(sourceNodeScope, destNodesScope, destIndex)
Check if the current dragging node can be dropped in the `ui-tree-nodes`.

**Parameters:**
- `sourceNodeScope`: The scope of source node which is dragging.
- `destNodesScope`: The scope of `ui-tree-nodes` which you want to drop in.
- `destIndex`: The position you want to drop in.

**Return**
If the nodes accept the current dragging node.
- `true` Allow it to drop.
- `false` Not allow.

##### <a name="dropped"></a>dropped(event)
If a node moves it's position after dropped, the `nodeDropped` callback will be called.

**Parameters:**
- <a name="eventParam"></a>`event`: Event arguments, it's an object.
  * `source`: Source object
    + `nodeScope`: The scope of source node which was dragged.
    + `nodesScope`: The scope of the parent nodes of source node  when it began to drag.
    + `index`: The position when it began to drag.
  * `dest`: Destination object
    + `nodesScope`: The scope of `ui-tree-nodes` which you just dropped in.
    + `index`: The position you dropped in.
  * `elements`: The dragging relative elements.
    + `placeholder`: The placeholder element.
    + `dragging`: The dragging element.
  * `pos`: Position object.

##### <a name="dragStart"></a>dragStart(event)
The `dragStart` function is called when the user starts to drag the node.
**Parameters:**
Same as [Parameters](#eventParam) of dropped.

##### dragMove(event)
The `dragMove` function is called when the user moves the node.

**Parameters:**
Same as [Parameters](#eventParam) of dropped.

##### dragStop(event)
The `dragStop` function is called when the user stop dragging the node.

**Parameters:**
Same as [Parameters](#eventParam) of dropped.

##### beforeDrop(event)
The `beforeDrop` function is called before the dragging node is dropped.

**Parameters:**
Same as [Parameters](#eventParam) of dropped.

### ui-tree-nodes
`ui-tree-nodes` is the container of nodes. Every `ui-tree-node` should have a `ui-tree-nodes` as it's container, a `ui-tree-nodes` can have multiple child nodes.

#### Attributes
##### data-nodrop<a name="nodes_attrs_nodrop"></a>
Turn off drop of nodes.
##### data-max-depth<a name="nodes_attrs_maxDepth"></a>
Number of levels a nodes can be nested (default 0). 0 means no limit. It can override the `data-max-depth` in `ui-tree`.
**Note**
If you write your own [$callbacks.accept](#accept) method, you have to check `data-nodrop` and `data-max-depth` by yourself.

Example: turn off drop.
```html
<ol ui-tree-nodes ng-model="nodes" data-nodrop>
  <li ng-repeat="node in nodes" ui-tree-node>{{node.title}}</li>
</ol>
```

#### Properties of scope
##### $element (type: AngularElement)
The html element which bind with the `ui-tree-nodes` scope.

##### $modelValue (type: Object)
The data which bind with the scope.

##### $nodes (type: Array)
All it's child nodes. The type of child node is scope of `ui-tree-node`.

##### $nodeScope (type: Scope of ui-tree-node)
The scope of node which current `ui-tree-nodes` belongs to.
For example:

    ui-tree-nodes                       --> nodes 1
      ui-tree-node                      --> node 1.1
        ui-tree-nodes                   --> nodes 1.1
          ui-tree-node                  --> node 1.1.1
          ui-tree-node                  --> node 1.1.2
      ui-tree-node                      --> node 1.2

The property `$nodeScope of` `nodes 1.1` is `node 1.1`. The property `$nodes` of `nodes 1.1` is [`node 1.1.1`, `node 1.1.2`]

##### maxDepth
Number of levels a node can be nested. It bases on the attribute [data-max-depth](#nodes_attrs_maxDepth).

##### nodrop
Turn off drop on nodes. It bases on the attribute [data-nodrag](#nodes_attrs_nodrop).

#### Methods of scope
##### depth()
Get the depth.

##### outOfDepth(sourceNode)
Check if depth limit has reached

##### isParent(nodeScope)
Check if the nodes is the parent of the target node.
**Parameters:**
- `nodeScope`: The target node which is used to check with the current nodes.


### ui-tree-node
A node of a tree. Every `ui-tree-node` should have a `ui-tree-nodes` as it's container.

#### Attributes
##### data-nodrag
Turn off drag of node.
Example: turn off drag.
```html
<ol ui-tree-nodes ng-model="nodes">
  <li ng-repeat="node in nodes" ui-tree-node data-nodrag>{{node.title}}</li>
</ol>
```

##### data-collapsed
Collapse the node.

#### Properties of scope
##### $element (type: AngularElement)
The html element which bind with the `ui-tree-nodes` scope.

##### $modelValue (type: Object)
The data which bind with the scope.

##### collapsed (type: Bool)
If the node is collapsed

- `true`: Current node is collapsed;
- `false`: Current node is expanded.

##### $parentNodeScope (type: Scope of ui-tree-node)
The scope of parent node.

##### $childNodesScope (type: Scope of ui-tree-nodes)
The scope of it's `ui-tree-nodes`.

##### $parentNodesScope (type: Scope of ui-tree-nodes)
The scope of it's parent `ui-tree-nodes`.

For example:

    ui-tree-nodes                       --> nodes 1
      ui-tree-node                      --> node 1.1
        ui-tree-nodes                   --> nodes 1.1
          ui-tree-node                  --> node 1.1.1
          ui-tree-node                  --> node 1.1.2
      ui-tree-node                      --> node 1.2

- `node 1.1.1`.`$parentNodeScope` is `node 1.1`.
- `node 1.1`.`$childNodesScope` is `nodes 1.1`.
- `node 1.1`.`$parentNodesScope` is `nodes 1`.

#### Methods of scope
##### collapse()
Collapse current node.

##### expand()
Expand current node.

##### toggle()
Toggle current node.

##### remove()
Remove current node.

##### depth()
Get the depth of the node.

##### maxSubDepth()
Get the max depth of all the child nodes. If there is no child nodes, return 0.

##### isSibling(targetNodeScope)
Check if the current node is sibling with the target node.
**Parameters:**
- `targetNodeScope`: The target node which is used to check with the current node.

##### isChild(targetNodeScope)
Check if the current node is a child of the target node.
**Parameters:**
- `targetNodeScope`: The target node which is used to check with the current node.


### ui-tree-handle
Use the `ui-tree-handle` to specify an element used to drag the object. If you don't add a `ui-tree-handle` for a node, the entire node can be dragged.

## NgModules Link

[Give us a like on ngmodules](http://ngmodules.org/modules/angular-ui-tree)

## Development environment setup
#### Prerequisites

* [Node Package Manager](https://npmjs.org/) (NPM)
* [Git](http://git-scm.com/)

#### Dependencies

* [Grunt](http://gruntjs.com/) (task automation)
* [Bower](http://bower.io/) (package management)

#### Installation
Run the commands below in the project root directory.

#####1. Install Grunt and Bower

    $ sudo npm install -g grunt-cli bower

#####2. Install project dependencies

    $ npm install
    $ bower install

## Useful commands

####Running a Local Development Web Server
To debug code and run end-to-end tests, it is often useful to have a local HTTP server. For this purpose, we have made available a local web server based on Node.js.

To start the web server, run:

    $ grunt webserver

To access the local server, enter the following URL into your web browser:

    http://localhost:8080/demo/

By default, it serves the contents of the demo project.


####Building angular-ui-tree
To build angular-ui-tree, you use the following command.

    $ grunt build

This will generate non-minified and minified JavaScript files in the `dist` directory.

####Run tests
You can run the tests once or continuous.

    $ grunt test
    $ grunt test:continuous
