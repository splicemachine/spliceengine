package com.splicemachine.derby.hbase;

/**
 * Abstraction for translating Hbase-specific Exceptions.
 *
 * @author Scott Fines
 *         Date: 12/3/14
 */
public interface ExceptionTranslator {

    /**
     * Determine if the specified error is in the category of
     * "infinitely retryable" exceptions. These are generally errors
     * of the category to which we know the system will "eventually"
     * deal with--moved or splitting regions, etc.
     *
     * Note that just because we can infinitely retry it does not mean that
     * the retry can be performed where you check it. If it requires a transactional
     * retry (e.g. {@link #needsTransactionalRetry(Throwable)} is true), then the retry
     * MUST be handled at the task framework level).
     *
     * @param t the error to check
     * @return true if the exception can be retried an infinite number of times
     */
    boolean canInfinitelyRetry(Throwable t);

    /**
     * Determine if the specified error is in the category of
     * "finitely retryable" exceptions. These are generally environmental
     * errors which we *hope* will resolve themselves relatively quickly, but
     * which must result in a failure if they do not.
     *
     * Included in this category (but not limited) are things like
     *
     * ConnectionRefused and normal IOExceptions
     *
     * Note that just because we can finitely retry it does not mean that
     * the retry can be performed where you check it. If it requires a transactional
     * retry (e.g. {@link #needsTransactionalRetry(Throwable)} is true), then the retry
     * MUST be handled at the task framework level).
     *
     * @param t the exception to check
     * @return true if the task can be retried a finite number of times.
     */
    boolean canFinitelyRetry(Throwable t);

    /**
     * @param t the exception to check
     * @return true if the specified exception requires that we roll back
     * a transaction before retrying. This allows us to ensure that exceptions
     * which are retryable are retried in the proper location (e.g. at the task
     * level instead of during the write pipeline).
     */
    boolean needsTransactionalRetry(Throwable t);

    boolean isCallTimeoutException(Throwable t);
    boolean isNotServingRegionException(Throwable t);
    boolean isWrongRegionException(Throwable t);
    boolean isRegionTooBusyException(Throwable t);
    boolean isInterruptedException(Throwable t);
    boolean isConnectException(Throwable t);
    boolean isIndexNotSetupException (Throwable t);
    boolean isPleaseHoldException(Throwable t);
    boolean isFailedServerException(Throwable t);
}
