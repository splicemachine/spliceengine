/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

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