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
 * Exists and Not-Exist flattening described in terms of AST.
 *
 *
 * EXISTS:
 *
 * select * from A join B on a1=b1 join C on b1=c1 where EXISTS(select 1 from Z where z1=a1);
 *
 * <pre>
 *
 *      J
 *     / \
 *    J   Z
 *   / \
 *  J  A
 * / \
 * B C
 *
 * </pre>
 *
 * Where the top join is an INNER join that emits at most one row for each matching row for join predicate z1=a1;
 *
 *
 * NOT-EXISTS:
 *
 * select * from A join B on a1=b1 join C on b1=c1 where EXISTS(select 1 from Z where z1=a1);
 *
 * <pre>
 *
 *      J
 *     / \
 *    J   Z
 *   / \
 *  J  A
 * / \
 * B C
 *
 * </pre>
 *
 * Where the top join is an LEFT OUTER join that emits one row on the left when there is no matching row on the right.
 *
 *
 * In practice Z is often multiple tables.  We currently place a distinct node above Z do prevent duplicate rows.
 */
package com.splicemachine.db.impl.sql.compile.subquery.exists;