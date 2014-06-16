package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import com.splicemachine.encoding.debug.BitFormat;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

/**
 *
 */
public class DNCTest {

    @Test
    public void test() {

        print(new BigDecimal("-555"));
        print(new BigDecimal("-55"));
        print(new BigDecimal("-6"));
        print(new BigDecimal("-5"));

        print(new BigDecimal("-.5"));
        print(new BigDecimal("-.05"));
        print(new BigDecimal("-.006"));
        print(new BigDecimal("-.005"));

        print(new BigDecimal("0"));

        print(new BigDecimal(".005"));
        print(new BigDecimal(".006"));
        print(new BigDecimal(".05"));
        print(new BigDecimal(".5"));


        print(new BigDecimal("5"));
        print(new BigDecimal("6"));
        print(new BigDecimal("55"));
        print(new BigDecimal("555"));

    }

    @Test
    public void test2() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-.5555555555555555555555555555555555"),
                new BigDecimal("-.5555555555555555"),
                new BigDecimal("-.555555555555555"),
                new BigDecimal("-.55555555555555"),
                new BigDecimal("-.5555555555555"),
                new BigDecimal("-.555555555555"),
                new BigDecimal("-.55555555555"),
                new BigDecimal("-.5555555555"),
                new BigDecimal("-.555555555"),
                new BigDecimal("-.55555555"),
                new BigDecimal("-.5555555"),
                new BigDecimal("-.555555"),
                new BigDecimal("-.55555"),
                new BigDecimal("-.5555"),
                new BigDecimal("-.555"),
                new BigDecimal("-.55"),
                new BigDecimal("-.5"),
                new BigDecimal("0")
        );
        for (BigDecimal d : bigDecimalList) {
            print(d);
        }
    }

    private void print(BigDecimal d) {
        System.out.println("" +
                        ", unscaled= " + d.unscaledValue() +
                        ", precision= " + d.precision() +
                        ", scale= " + d.scale() +
                        ", exponent= " + (d.precision() - d.scale() - 1) +
                        ", value=" + d +
                        ", encoded=" + new BitFormat().format(BigDecimalEncoding.toBytes(d, false))
        );
    }


}
