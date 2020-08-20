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

package com.splicemachine.encoding;

import splice.com.google.common.collect.Lists;
import com.splicemachine.utils.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 7/31/13
 */
@RunWith(Parameterized.class)
public class MultiFieldEncoder_RandomizedTest {
    private static final int NUM_RANDOM_TESTS = 10;
    private static final int MAX_FIELDS_PER_TEST = 10;

    @SuppressWarnings("unchecked")
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayListWithCapacity(NUM_RANDOM_TESTS);
        //Test just serializing a single field in all possible combinations
        Random random = new Random(0l);
        for(TestType type:TestType.values()){
            data.add(new Object[]{Arrays.asList(Pair.newPair(type, type.generateRandom(random)))});
        }

        //add some fixed tests for known edge cases

        /*
         * -8.6...E307 encodes to [0,32,0,0,0,0,0,0], which has a leading zero. This is problematic
         * if all you do is check the next entry for zeros for nullity
         */
        data.add(new Object[]{
                Arrays.asList(Pair.newPair(TestType.DOUBLE,Double.parseDouble("-8.98846567431158E307")),
                        Pair.newPair(TestType.BOOLEAN,true))
        });

        /*
         * -Infinity encodes to [0,-128,0,0], which has a leading zero. This is problematic if all you
         * do is check the next entry for zeros for nullity
         */
        data.add(new Object[]{
                Arrays.asList(Pair.newPair(TestType.FLOAT,Float.parseFloat("-Infinity")),
                        Pair.newPair(TestType.BOOLEAN,true))
        });

        /*
         * -57 was at one point encoding to 0x00 in descending order, which causes the MultiFieldDecoder
         * to break. Leave this in to make sure it doesn't happen again.
         */
        List<Pair<TestType,? extends Serializable>>  dataPair = Arrays.asList(Pair.newPair(TestType.INTEGER,-1985683736),
                Pair.newPair(TestType.SORTED_BYTES,new byte[]{-116,34,-81,77,103,-57}));
        data.add(new Object[]{dataPair});

        //test all possible combinations of two fields
        for(TestType type:TestType.values()){
            for(TestType secondTestType:TestType.values()){
                data.add(new Object[]{Arrays.asList(Pair.newPair(type,type.generateRandom(random)),
                        Pair.newPair(secondTestType,secondTestType.generateRandom(random)))});
            }
        }

        //test all combinations of three fields
//        data.add(new Object[]{Arrays.asList(Pair.newPair(TestType.BOOLEAN,TestType.BOOLEAN.generateRandom(random)),
//                Pair.newPair(TestType.RAW_BYTES,TestType.RAW_BYTES.generateRandom(random)),
//                Pair.newPair(TestType.NULL,TestType.NULL.generateRandom(random)))});
         for(TestType type:TestType.values()){
            for(TestType secondTestType:TestType.values()){
                for(TestType thirdTestType:TestType.values()){
                    data.add(new Object[]{Arrays.asList(Pair.newPair(type,type.generateRandom(random)),
                         Pair.newPair(secondTestType,secondTestType.generateRandom(random)),
                            Pair.newPair(thirdTestType,thirdTestType.generateRandom(random)))});
                }
            }
        }

        //you can't test for combinations of 4 fields with the default Java Heap Size--there are too many permutations
        //if you want to test it, up your heap size, and uncomment the following section.
//        for(TestType type:TestType.values()){
//            for(TestType secondTestType:TestType.values()){
//                for(TestType thirdTestType:TestType.values()){
//                    for(TestType fourthTestType:TestType.values()){
//                    data.add(new Object[]{Arrays.asList(Pair.newPair(type,type.generateRandom(random)),
//                            Pair.newPair(secondTestType,secondTestType.generateRandom(random)),
//                            Pair.newPair(thirdTestType,thirdTestType.generateRandom(random)),
//                        Pair.newPair(fourthTestType,fourthTestType.generateRandom(random)))});
//                    }
//                }
//            }
//        }

        return data;
    }

    private final List<Pair<TestType,Object>> types;

    public MultiFieldEncoder_RandomizedTest(List<Pair<TestType, Object>> types) {
        this.types = types;
    }

    @Test
    public void testCanEncodeAndDecodeAllFieldsCorrectlyDescending() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(types.size());
        int i=0;
        for(Pair<TestType,Object> type:types){
            TestType t = type.getFirst();
            Object c = type.getSecond();
            t.load(encoder,c,i%2!=0);
            i++;
        }

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(encoder.build());
        i =0;
        for(Pair<TestType,Object> cTestType:types){
            TestType cT = cTestType.getFirst();
            Object correct = cTestType.getSecond();
            cT.check(decoder,correct,i%2!=0);
            i++;
        }
    }

    @Test
    public void testCanEncodeAndDecodeAllFieldsCorrectly() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(types.size());
        for(Pair<TestType,Object> type:types){
            TestType t = type.getFirst();
            Object c = type.getSecond();
            t.load(encoder,c);
        }

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(encoder.build());
        for(Pair<TestType,Object> cTestType:types){
            TestType cT = cTestType.getFirst();
            Object correct = cTestType.getSecond();
            cT.check(decoder,correct);
        }
    }
}
