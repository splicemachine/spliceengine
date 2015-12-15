package com.splicemachine.pipeline.writecontextfactory;

/*
 * State management indicator
 */
enum State {
    /*
     * Mutations which see this state will emit a warning, but will still be allowed through.
     * Allowing mutations through during this phase prevents deadlocks when initialized the Database
     * for the first time. After the Derby DataDictionary is properly instantiated, no index set should
     * remain in this state for long.
     */
    WAITING_TO_START,
    /*
     * Indicates that the Index Set is able to start whenever it feels like it (typically before the
     * first mutation to the main table). IndexSets in this state believe that the Derby DataDictionary
     * has properly instantiated.
     */
    READY_TO_START,
    /*
     * Indicates that this set is currently in the process of intantiating all the indices, constraints, and
     * so forth, and that all mutations should block until set up is complete.
     */
    STARTING,
    /*
     * Indicates that initialization failed in some fatal way. Mutations encountering this state
     * will blow back on the caller with a DoNotRetryIOException
     */
    FAILED_SETUP,
    /*
     * The IndexSet is running, and index management is going smoothly
     */
    RUNNING,
    /*
     * the IndexSet is shutdown. Mutations will explode upon encountering this.
     */
    SHUTDOWN,
    /*
     * This IndexSet does not manage Mutations.
     */
    NOT_MANAGED
}
