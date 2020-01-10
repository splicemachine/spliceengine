/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import java.lang.annotation.*;

import static java.lang.annotation.ElementType.*;

/**
 * Used to indicate that a given class or method has not been tested
 *  fully (to the knowledge of the author) and that extra care should
 *  be taken when using that method or class.
 *
 *  Essentially, this is a "Use at your own risk" warning--when you use
 *  something like this, and your code breaks, then you need to fix it and
 * add some tests.
 *
 * @author Scott Fines
 *         Date: 2/19/15
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(value={METHOD,TYPE})
public @interface Untested { }
