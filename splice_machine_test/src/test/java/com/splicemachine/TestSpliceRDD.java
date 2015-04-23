package com.splicemachine;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * Created by jleach on 4/9/15.
 */
public class TestSpliceRDD {

    @Test
    public void testSpliceRDD() {



        ArrayList<String> foo2 = new ArrayList<>();
        foo2.add("foo");
        foo2.add("foo1");
        foo2.add("foo2");

        Iterable<String> foo = (Iterable) foo2;

        foo = Iterables.transform(foo,new Function<String, String>() {
            @Override
            public String apply(@Nullable String s) {
                System.out.println ("1");
                return s;
            }
        });

        foo = Iterables.transform(foo,new Function<String, String>() {
            @Override
            public String apply(@Nullable String s) {
                System.out.println ("2");
                return s;
            }
        });

        System.out.println ("Begin");
        for (String fooString : foo) {
            System.out.println(fooString);
        }
        System.out.println("End");



//        SpliceRDD test = new SpliceRDD(null,null);
 //       test.toLocalIterator();
    }

}
