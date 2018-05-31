//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.google.common.base;

import com.google.common.annotations.Beta;
import com.google.common.annotations.GwtCompatible;
import javax.annotation.Nullable;

@Beta
@GwtCompatible
public class VerifyException extends RuntimeException {
    public VerifyException() {
    }

    public VerifyException(@Nullable String message) {
        super(message);
    }

    public VerifyException(@Nullable Throwable cause) {
        super(cause);
    }

    public VerifyException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}

