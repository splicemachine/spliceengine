package com.splicemachine.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author Scott Fines
 *         Date: 2/12/14
 */
public class UnsafeUtil {
		/** Fetch the Unsafe.  Use With Caution. */
		public static Unsafe unsafe() {
				// Not on bootclasspath
				if( UnsafeUtil.class.getClassLoader() == null )
						return Unsafe.getUnsafe();
				try {
						final Field fld = Unsafe.class.getDeclaredField("theUnsafe");
						fld.setAccessible(true);
						return (Unsafe) fld.get(UnsafeUtil.class);
				} catch (Exception e) {
						throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e);
				}
		}
}
