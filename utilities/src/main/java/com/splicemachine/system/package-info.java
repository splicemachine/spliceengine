/**
 * Classes in this package are responsible for determining the System configuration for a given JVM.
 *
 * <p>
 *     System configurations are generally regarded as any configuration which isn't part of the application
 *     configuration, but which may still have an effect on the performance and/or stability of the system. For example,
 *     the Garbage Collection configuration is a system configuration, but how many threads to use in a specific
 *     thread pool might not be. Several examples of System configurations are
 *
 *     <ul>
 *         <li>Disk space</li>
 *         <li>Number of CPU Cores</li>
 *         <li>Total Heap Available</li>
 *         <li>Total System memory available(i.e. RAM)</li>
 *     </ul>
 *
 *     Although there are many others as well. Note that sometimes these configurations can only be determined
 *     approximately, and only based on that which Java provides (For example, sometimes Java will report the number
 *     of logical cores, instead of physical ones); sometimes these configurations are not 100% correct,
 *     but we may a best effort to determine them as accurately as possible.
 * </p>
 *
 * <p>
 *     The contents of this package are primarily interested in measuring and reporting System configurations
 *     in a reasonable and programmatic way, so that it can be easily recorded by both programs and people. It
 *     would be very rare indeed for a class in this package to be able to modify these parameters, as almost
 *     all should be either a fact about the available hardware, or some other form of configuration which
 *     cannot be changed without a restart.
 * </p>
 *
 * @author Scott Fines
 * Date: 1/13/15
 */
package com.splicemachine.system;