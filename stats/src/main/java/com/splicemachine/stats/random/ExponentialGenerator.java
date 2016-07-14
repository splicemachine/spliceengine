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

package com.splicemachine.stats.random;

/**
 * @author Scott Fines
 *         Date: 12/2/14
 */
public class ExponentialGenerator extends ZigguratGenerator{
    public ExponentialGenerator(RandomGenerator baseRandom) {
        super(baseRandom);
    }

    @Override protected double tail(double u0, double u1) { return this.x[0]-Math.log(u1); }
    @Override protected double phiInverse(double v) { return -Math.log(v); }

    @Override
    protected double phi(double x) {
        return Math.exp(-x);
    }

    /*Constants taken from Marsaglia et al (http://www.jstatsoft.org/v05/i08/paper/)*/
    @Override protected double x0() { return 7.69711747013104972; }
    @Override protected double area() { return .0039496598225815571993; }
}
