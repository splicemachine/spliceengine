#Proposal for improved testability of SpliceMachine

+ Axiom #1: If testing isn't easy to do, it won't be done, or it won't be done well.
+ Axiom #2: Delaying or ignoring testing in favor of new features will almost certainly delay future releases
+ Axiom #3: The faster a test runs, the faster the debug cycle, and the faster a developer solves the issue

##Problems

1. Too Complex: 
	+ The global system is too complex to easily understand
	+ Requires months to come up to speed
	+ Requires significant experience to diagnose unusual issues
	+ Unpredictable timing issues (around concurrency, etc) make finding even deterministic elements difficult
	+ You can't test the optimizer without having a running HBase instance, which means that John breaking HBase will make my testing the optimizer nearly impossible until he gets it fixed(for example).
2. Too Slow
	+ Testing a new feature or bug fix requires starting an instance, and trying it out. Making sure it works involves starting a *clean* instance, which takes a very long time
3. Too difficult to test
	+ Only Integration Tests are writable--this means that NullPointers, and other minor issues are difficult to fully flesh out
	+ Parametric tests are nearly impossible: For example, testing that we can read every data type properly requires tons of manual coding effort, which is not likely to happen

##Proposal

1. Split SpliceMachine into separable *main modules* 
	+ Optimizer
	+ Logical Execution Engine
	+ Write Pipeline
	+ Transactions
	+ Read pipeline (i.e. HBase scanning, HFile reading, Spark reading, etc)
	+ Client operations
	+ Operation Control and Coordination

###Module Properties

1. Each module must be able to run _in isolation_ from all other modules. For example, The optimizer should not require a JDBC client _or_ a running HBase instance in order to boot and run tests.
2. Each module has a minimum _clear_ dependency chain on the other modules. It must be possible to outline which main modules are necessary to test which components
3. It must be possible to start a module and it's dependencies in a "partial" mode. For example, If the Logical Execution Engine depends on the Operation Control module, but not on the Client, it should be possible to run a test which have the real LEE and OC modules, but which have a faked (or non-present) client module.
4. independent Unit tests are written for _each_ module.
5. Where reasonable, modules should reside within their own git repository. 
	+ This is important because it will allow developers to write extensive (and potentially long-running) tests within each module without fear of making existing builds longer

### Current Status
+ Transaction is a distinct module
	+ Note that some code exists in the transaction module which does not belong there (generic HBase stuff). This code would need to be moved
+ Encoding/Decoding would be easy to pull out

## Needed work

### Optimizer Module
#### Properties:
1. Optimizer depends on HBase:
	+ DataDictionary
	+ Collected Statistics
2. Depends on ZooKeeper:
	+ Transaction generation
	+ Properties
3. Depends on Timestamp Server(HMaster)
	+ Transaction Generation

#### Steps to isolate:
1. Create a simulated in-memory DataDictionary
2. Create a simulated in-memory TransactionOracle
3. Create a mechanism to _quickly_ boot the needed Derby classes _without_ writing data to disk
4. Provide a simulated Statistics tool, so that collection is not necessary

### Execution Engine
1. Separate Logical execution (i.e. manipulating ExecRows) from physical layout (i.e. encoding/decoding)
2. Separate each operation from underlying: For example, write a way to test a ProjectRestrict which does not require a table scan
3. Remove immediate activation dependency: Allow test code to inject direct code processing instead of generated code when needed

###Write Pipeline
1. Simulated HBase network stack: Framework to simulate Timeouts, Server failures, etc. in a predictable manner
2. Simulated HBase Region code
3. Ability to test write pipeline without transactional code (i.e. swap out transactional interfaces with no-ops where necessary)

### Transactions
1. Remove raw HBase client code from Transactional code base

### Read Pipeline
1. Isolate Read code behind well-defined interfaces
2. Provide simulated RegionScanners to test region splitting, region failing, etc

### Client Operations
1. As of current code, JDBC client is well separated. This should be maintained in the future

### Operation Control
1. Provide interface isolation for Task and Spark-based coordinations
2. Simulate job submission and execution with local, in memory execution

### General work
1. Many algorithms are multi-threaded by design: we must isolate those ThreadPools so that they can be controlled by testers quickly and easily
2. Many small dependencies all over the place: How does one construct an HBaseAdmin properly? This basic question is not easily answered currently

##General Notes
1. Should be broken into major components over the next 6 months-1 year, with _determined_ focus to accomplish one separation each cycle
2. Care must be taken that needed refactoring does not negatively impact overall performance
3. Integration Tests will still be necessary in many cases
	+ Should consider overhaul of IT framework as well to improve readability and ease of use in line with Axiom 1
	+ Scala as a testing language? Python? Anything that makes writing tests faster and simpler
4. Senior developers (read: John and I) must make it an _absolute priority_ to abide by these principles. 

##Development Roadmap:
1. Optimizer separation
2. Logical execution separation (a la datasets etc)
3. Simulated HBase
4. Read Pipeline cleanup
5. Write Pipeline cleanup
6. Operation coordination

I believe this to be extremely important--I would like to take primary responsibility, coordinating with Walt, Jeff, and Keith in a rotating basis.

