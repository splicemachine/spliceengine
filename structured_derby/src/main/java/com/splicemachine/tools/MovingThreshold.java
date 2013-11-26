package com.splicemachine.tools;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 * Date: 11/26/13
 */
public class MovingThreshold implements OptimizingValve.Threshold{
		public static enum OptimizationStrategy{
				CONSTANT,
				MINIMIZE{
						@Override
						public OptimizingValve.Position optimize(int distance) {
								if(distance>1) return OptimizingValve.Position.HIGH;
								else if(distance<-1){
										//we got lower on our last run, so suggest going lower again
										return OptimizingValve.Position.HIGH;
								}
								else return OptimizingValve.Position.OK;
						}
				},
				MAXIMIZE{
						@Override
						public OptimizingValve.Position optimize(int distance) {
								if(distance<-1) return OptimizingValve.Position.LOW;
								else if(distance>1){
										//we got lower on our last run, so suggest going higher again
										return OptimizingValve.Position.LOW;
								}
								else return OptimizingValve.Position.OK;
						}
				};

				public OptimizingValve.Position optimize(int distance){
						if(distance<-1) return OptimizingValve.Position.LOW;
						else if(distance>1) return OptimizingValve.Position.HIGH;
						else return OptimizingValve.Position.OK;
				}
		}

		private final int windowSize;
		private final OptimizationStrategy optimizationStrategy;
		private volatile WindowedAverage threshold;
		private AtomicReference<WindowedAverage> currentWindow;


		public MovingThreshold(OptimizationStrategy optimizationStrategy,int windowSize) {
				this.windowSize= windowSize;
				this.optimizationStrategy = optimizationStrategy;
				threshold = new WindowedAverage(windowSize);
				currentWindow = new AtomicReference<WindowedAverage>(threshold);
		}

		@Override
		public OptimizingValve.Position exceeds(double measure) {
				/*
				 * The strategy is:
				 *
				 * 1. Update the current window.
				 * 2. If the current window is also the threshold window:
				 * 	A. if the threshold window is full, then attempt to swap the current window
				 * 		with the threshold window
				 */
				WindowedAverage window = currentWindow.get();
				window.update(measure);
				if(window==threshold){
						if((window.numberOfMeasurements() & (windowSize-1))==0){
								WindowedAverage avg = new WindowedAverage(windowSize);
								/*
								 * Attempt to set up a new windowed average, but if someone else wins,
								 * you don't need to retry it.
								 */
								currentWindow.compareAndSet(window,avg);
						}
						//while the threshold is being built, we don't want to make any adjustments
						return OptimizingValve.Position.OK;
				}else if ((window.numberOfMeasurements() & (windowSize-1))==0){
						/*
						 * Our rolling average may be higher or lower than the threshold average (in fact,
						 * it's likely to be). If it is far enough away(greater than 1 std. Dev.) then we
						 * attempt to adjust the position up or down.
						 */
						double thresholdMean = threshold.estimateMean();
						double thresholdDeviation = threshold.estimateDeviation();
						double currentMean = window.estimateMean();

						double diff = currentMean-thresholdMean;
						int devDiff = (int)(diff/thresholdDeviation);
						return optimizationStrategy.optimize(devDiff);
				}else
						return OptimizingValve.Position.OK; //don't change the position until we have a full window
		}

		public void reset(){
				WindowedAverage newThreshold = new WindowedAverage(windowSize);
				threshold = newThreshold;
				currentWindow.set(newThreshold);
		}

		public static void main(String...args) throws Exception{
				MovingThreshold threshold = new MovingThreshold(OptimizationStrategy.MINIMIZE,8);

				double[] baseLatencies = new double[]{100,200,150,160,185,137,98,114,
								247,378,521,600,635,427,213,
								100,110,84,72,130,60,53,90};
				double volatility = 100d;
				Random random = new Random(0l);
				int size = 100;
				for(int i=0;i<size;i++){
						double latency = baseLatencies[i%baseLatencies.length]+random.nextDouble()*volatility;

						OptimizingValve.Position position = threshold.exceeds(latency);
						if(position== OptimizingValve.Position.HIGH)
								System.out.printf("HIGH at %d%n",i);
						if(position== OptimizingValve.Position.LOW)
								System.out.printf("LOW at %d%n", i);
				}

		}
}
