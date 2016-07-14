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
public class GaussianGenerator extends ZigguratGenerator{
    public GaussianGenerator(RandomGenerator baseRandom) {
        super(baseRandom);
    }

    @Override
    public double nextDouble() {
        double d= super.nextDouble();
        if(baseRandom.nextBoolean()) return -d;
        return d;
    }


    @Override
    protected double tail(double u0, double u1) {
        double x = -Math.log(u0)/this.x[0];
        double y = -Math.log(u1);
        if(2*y>Math.pow(x,2)) return x+this.x[0];
        else return nextValue(u0,u1); //warning, recursive call here!
    }

    @Override
    protected double phiInverse(double v) {
        return Math.sqrt(-2*Math.log(v));
    }

    @Override
    protected double phi(double x) {
        return Math.exp(-Math.pow(x,2)/2);
    }

    /*Constants taken from Marsaglia et al (http://www.jstatsoft.org/v05/i08/paper/)*/
    @Override protected double x0()   { return 3.6541528853610088; }
    @Override protected double area() { return  .00492867323399  ; }
}
