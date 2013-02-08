
Deploy Snapshots:

mvn -DskipTests clean javadoc:jar source:jar javadoc:test-jar source:test-jar deploy

Deploy Release (Hopefully we can remove the skipTests portion soon):

 mvn -Darguments="-DskipTests" release:prepare
 
 mvn -Darguments="-DskipTests" release:perform