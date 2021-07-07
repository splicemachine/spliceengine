package com.splicemachine.derby.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD", "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class CSuperClassExample {
    public CSuperClassExample() {
        str = "Hello World";
        a = 123;
        sarr = new Integer[]{0, 10, 20, 30, 40, 50};
    }
    public Integer a;
    public Integer b;
    public String str;
    public Integer sarr[];
    public int i;

    public void staticFunc(int a, int b, int c) {
        this.a = a+b+c;
    }

    public void staticFunc2(int a, Integer b, int c) {
        this.a = a+b+c;
    }
}
