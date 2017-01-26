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

package com.splicemachine.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * Annotation to indicate that a given class,method, etc. is ThreadSafe.
 *
 * When specified on a Parameter, then it is an indication that that
 * parameter may be used by multiple threads, and thus should be thread safe.
 *
 * When specified on a Class, it is an indication that the class is thread-safe.
 *
 * When specified on an interface, it is an indication that all implementations MUST
 * be thread-safe.
 *
 * @author Scott Fines
 * Date: 1/24/14
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(value={FIELD,METHOD,PARAMETER,TYPE})
public @interface ThreadSafe { }
