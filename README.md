# db-project


Splice fork of apache derby (10.9.1.0).


## build

mvn clean install


## modules


* **db-build**: Classes used as part of build.
* **db-client**: Splice JDBC driver.
* **db-drda**: DRDA classes.
* **db-engine**: Server, compiler, optimizer, etc.
* **db-shared**: Code shared by client/server.
* **db-tools-i18n**: Internationalization
* **db-tools-ij**: Our IJ implementation.
* **db-tools-testing**: Testing code shared by test cases in all modules.
* **db-testing**: Old integration tests that should be deleted or moved to spliceengine repo.


