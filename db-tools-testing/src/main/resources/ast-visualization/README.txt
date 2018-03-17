Usage
=====

* Visit Hbase admin console: http://localhost:60010/master-status

* Click "Log Level"

* Set com.splicemachine.db.impl.ast.JsonTreeBuilderVisitor to TRACE

* Run some SQL

* Find the JSON in: AFTER_BIND.json, AFTER_OPTIMIZE.json, AFTER_PARSE.json 

 (it attempts to write to the target sub director under splice process current directory, if it exists, else in the CWD.
 Feel free to improve this if you would like to see these saved somewhere else)  
  
* Open ast-visualization.html (In IntelliJ you can right click and select "Open In Browser..."

* Click "Chose File" and select the .json file to view.


TODO
====

* In GraphControl.js there is a method named draw() that creates an options map.  One of the options is
  groups.  Each named group corresponds to a node "group" (update, subquery, etc).  I have explicitly 
  set the size and color of a few node groups here.  You may want to add additional explicit group colors/sizes.
  
  http://visjs.org/docs/network/
