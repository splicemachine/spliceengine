package com.splicemachine.utils;

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
 * @author Scott Fines
 * Date: 1/24/14
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(value={FIELD,METHOD,PARAMETER,TYPE})
public @interface ThreadSafe { }
