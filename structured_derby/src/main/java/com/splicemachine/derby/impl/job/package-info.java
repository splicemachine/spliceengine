/**
 * Tools relating to asynchronous execution of multiple independent tasks
 * across many machines.
 *
 * There are two components to the framework: {@link com.splicemachine.job.Job} and
 * {@link com.splicemachine.job.Task}. Essentially, A {@code Job} is comprised of
 * one or more {@code Task}s, each of which perform an independent unit of computation.
 * The {@code Job} is then responsible for determining how {@code Task}s are
 * created and assigned.
 *
 * <b>Job Control</b>
 *
 * Job Control is maintained by a {@link com.splicemachine.job.JobScheduler}, which is responsible
 * for taking multiple tasks, submitting them, and managing their state information as they are returned.
 *
 * The canonical example of this is {@link com.splicemachine.derby.impl.job.scheduler.DistributedJobScheduler}, which
 * uses ZooKeeper as a distributed coordination mechanism. Tasks report their state to the JobScheduler by modifying
 * the state of a ZooKeeper node, which is watched by the JobScheduler. Once notified, the JobScheduler reacts
 * according to the changed state (i.e. committ/rollback, resubmission of retryable tasks, etc.).
 *
 * If all tasks execute successfully and are able to report success to the JobScheduler (perhaps after a few retries), then
 * the Job is considered to have executed successfully, and that information is propogated on to the Job submitter.
 *
 * If <em>any</em> task failed to complete after a (configurable) number of retries (if retryable), then the Job is considered
 * Failed, and information (as much as is available) on the failure will be propogated to the Job submitter.
 *
 * <b>Task Submission</b>
 * The maximum unit of computation for a single Task is a single Region--That is, if there are two regions of interest
 * to a Job, then two tasks <em>must</em> be submitted (This is usually much easier than it sounds to ensure, because of HBase's
 * coprocessor mechanism, which we'll get to in a second).
 *
 * Since Tasks are connected to a Region, The obvious transport for task submission is a Coprocessor, which is what happens. The
 * {@link com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler} is a Coprocessor which sits on any region
 * which accepts tasks via a coprocessor exec call, and submits those tasks to a Local Thread Pool for asynchronous execution.
 *
 * This submission first creates a ZooKeeper node which contains status information about that Task's state, and that
 * zookeeper node is returned to the JobScheduler for Job control. After the ZooKeeper node is created, the Task is enqueued
 * for asynchronous execution. Then it transitions its state to PENDING and waits for available resources
 *
 * <b>Task Execution</b>
 *
 * A task is executed as soon as it can be reached by an execution Thread. Tasks are executed in a Balanced Priority Manner--that
 * is, a Task has a priority associated with it; The higher the priority, the sooner a thread will be made available to it
 * (Note, however, that there is as yet no concept of pre-emption. A task cannot force another task to surrender its execution,
 * it must wait for currently executing tasks to complete. This has the consequence that if every task has the same priority,
 * then we degenerate to a normal First In, First Out queue).
 *
 * Once a thread is available to execute the task, the Task is moved into the EXECUTING state. This state is propagated to
 * ZooKeeper, then execution may commence. Execution occurs within a single thread (that is, Tasks are not required to be
 * thread-safe).
 *
 * If the Task fails for <em>any</em> reason, it is allowed to throw an ExecutionException wrapping it's error. That error
 * is captured, and the task is transitioned to the FAILED state, with the error attached. The JobScheduler will notice
 * this (via zookeeper watches), and will attempt to retry if the error is one that can be retried (if it cannot be, as
 * in the case of Primary Key Violations, the job will immediately fail, and the Scheduler will attempt to cancel all
 * uncompleted tasks for that Job).
 *
 * Error Handling
 *
 * The expectation is that a Task will throw an ExecutionException which contains reasonable information in the <em>message</em>
 * of the error. While the Stack trace may be logged by the Task execution framework, it is <em>not</em> guaranteed to be
 * delivered to the Job control. Only the type of the error and it's error message is allowed. This is because ZooKeeper has
 * a limit to how much data can be stored in a single znode (typically 1MB), and extra long stack traces sometimes go over
 * that limit. Thus, if you want a Task to deliver a human readable error to the job submitter, it is vitally important for that
 * message to be clear, concise, and descriptive, as the Stack Trace itself may not be available. However, the message is not
 * allowed to be too long either (so don't try and shove the entire stack trace into the message as a way of getting around this).
 *
 * <b>Transactional Support</b>
 *
 * The most natural way to model transactions in asynchronous tasks is to have a single
 * parent transaction responsible for the entire job, and each {@code Task} maintains
 * and controls it's own separate child transaction. In this model, each Task would
 *
 * <ol>
 *     <li>Create a new Child transaction</li>
 *     <li>Execute it's stuff</li>
 *     <li>(If execution succeeds) commit child transaction</li>
 *     <li>(If execution fails) rollback child transaction</li>
 *     <li>Report status back to Job control</li>
 * </ol>
 *
 * However, in practice, this can result in poor failure semantics.
 *
 * All communication between a {@code Task} and the Job control is distributed
 * in nature, which means it's error-prone. If the child task commits, but communication
 * between the task and the job control fails, it is possible for the job control
 * to decide that the task has failed, and to resubmit that task to a different node.
 * When that happens, the task is executed twice, even though it has committed the
 * first time, resulting in various different errors (depending on the Task). For example,
 * if the task is an import to a PrimaryKey-ed table, the result is an
 * extraneous PrimaryKey violation.
 *
 * Proper termination of a child transaction therefore <em>must</em> be managed
 * by the Job Control mechanism. This leads to the next logical model, which is:
 *
 * <ol>
 *     <li>Job creates multiple Tasks</li>
 *     <li>Job Control creates child transaction for each individual Task</li>
 *     <li>Job Control submits all tasks for execution</li>
 *     <li>As Each Task completes, it reports status back to JobControl</li>
 *     <li>(If Child task fails)JobControl rolls back the child transaction, then resubmits the task if possible</li>
 *     <li>(If Child task succeeds)JobControl commits the child transaction</li>
 *     <li>Once all Tasks have reported success and/or failure, the JobControl returns the final state of the operation</li>
 * </ol>
 *
 * However, this leads to a practical problem. In order to deal with the networked nature of Transaction Management (in particular,
 * random node death), we have a <em>Transaction timeout</em> which will essentially roll back a transaction that has been
 * inactive for a certain length of time. Since tasks are executed Asynchronously, and are queued until an execution resource
 * is available, there is an essentially arbitrary time window between when a task is submitted and when it is executed. This means
 * it is possible for a Child transaction to timeout before execution can even begin, resulting in errors.
 *
 * Therefore, Child transaction creation must occur as close to the actual execution as possible. The final model is then
 *
 * <ol>
 *     <li>Job creates multiple Tasks</li>
 *     <li>JobControl submits all tasks for execution</li>
 *     <li>When Task is given resources and executed, it first creates a Child transaction and propogates that back
 *     to the JobControl</li>
 *     <li>When Task completes, it notifies JobControl of success or failure</li>
 *     <li>When JobControl obtains success or failure of a Task, it either commits or rolls back the child transaction</li>
 * </ol>
 *
 * Which is frankly annoying--transactional control is spread between two separate machines, in two entirely separate organizational
 * units. However, it is necessary to avoid errors due to transactional race conditions.
 *
 */
package com.splicemachine.derby.impl.job;


