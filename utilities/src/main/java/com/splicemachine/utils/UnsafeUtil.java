/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author Scott Fines
 *         Date: 2/12/14
 */
public class UnsafeUtil {
		private static final Unsafe INSTANCE;
		private static final long BYTE_ARRAY_BASE_OFFSET;
		static{
				INSTANCE = getUnsafeInternal();
				BYTE_ARRAY_BASE_OFFSET = INSTANCE.arrayBaseOffset(byte[].class);
		}

		/** Fetch the Unsafe.  Use With Caution. */
		public static Unsafe unsafe() { return INSTANCE; }

		public static long byteArrayOffset(){
				return BYTE_ARRAY_BASE_OFFSET;
		}

		private static Unsafe getUnsafeInternal() {
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
