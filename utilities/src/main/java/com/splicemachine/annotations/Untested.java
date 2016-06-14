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
