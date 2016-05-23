/**
 * Everything in this package was added by splice.
 *
 * <pre>
 * <ul>
 *     <li>aggregate -- subquery flattening for subqueries that contain aggregates.</li>
 *     <li>exists -- subquery flattening for EXISTS subqueries.  Derby did not flatten exists subquries with more than
 * one table.</li>
 * </ul>
 * </pre>
 *
 *
 *
 * Derby's original flattening strategies are described at a high level here:
 *
 * http://db.apache.org/derby/docs/10.9/tuning/ctuntransform13699.html
 */
package com.splicemachine.db.impl.sql.compile.subquery;