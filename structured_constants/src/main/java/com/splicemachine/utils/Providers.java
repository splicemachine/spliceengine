package com.splicemachine.utils;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class Providers {

		private Providers(){}

		public static <T> Provider<T> basicProvider(final T entity){
			return new Provider<T>() {
					@Override
					public T get() {
							return entity;
					}
			};
		}
}
