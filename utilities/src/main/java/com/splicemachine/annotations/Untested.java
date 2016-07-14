/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
