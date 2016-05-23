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