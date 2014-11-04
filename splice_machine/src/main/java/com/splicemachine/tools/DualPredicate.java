package com.splicemachine.tools;

/**
 * @author Scott Fines
 * Date: 12/4/13
 */
public interface DualPredicate<T> {

		boolean apply(T one,T two);
}
