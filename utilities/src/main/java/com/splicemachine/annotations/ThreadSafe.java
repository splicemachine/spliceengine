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
