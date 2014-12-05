package com.splicemachine.stats.random;

/**
 * @author Scott Fines
 *         Date: 12/2/14
 */
public class ExponentialDistribution extends ZigguratDistribution{
    public ExponentialDistribution(RandomDistribution baseRandom) {
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
