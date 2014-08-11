package com.splicemachine.logicalstats.histogram;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * @author Scott Fines
 * Date: 4/1/14
 */
public class EquiDepthUtf8Histogram extends BaseObjectHistogram<String>{
		//The maximum number of possible UTF-8 values.
		private static final BigDecimal UTF_LENGTH = BigDecimal.valueOf(1111998);
		private final int width;

		private final MathContext slopeContext;

		EquiDepthUtf8Histogram(String[] buffer, int numBuckets, int stringWidth) {
				super(buffer, numBuckets);
				this.width = stringWidth;
				slopeContext = new MathContext(2 * width);
		}

		@Override
		protected int interpolate(String start, String stop, String element, int totalElements) {
				/*
				 * Interpolating Strings requires a bit of finagling with the mathematics.
				 *
				 * First, it's important to recognize that a String is a sequence of characters, each
				 * of which can take on values in UTF-8. Thus, each character in a string is only allowed
				 * a finite number of possible elements (approximately 1 million, represented as UTF_LENGTH).
				 *
				 * So, if we assign a number to each possible character in UTF-8, we can transform a string
				 * into a decimal number in base UTF_LENGTH, by the formula
				 *
				 * sum (c[i]*L^(-i))
				 *
				 * where i is the position of the character in the string, and c[i] is the UTF-8 codepoint
				 * of the character in the string (L = UTF_LENGTH).
				 *
				 * However, considering how large UTF_LENGTH is, these numbers would all be really small. To make
				 * that calculation easier, we scale the number up by the expected width of the string. This makes the
				 * numbers larger and thus less prone to round off errors in the high ends of the decimals, resulting in
				 *
				 * sum (c[i]*L^(width-i))
				 *
				 * Unfortunately, when width>2, UTF_LENGTH^width overflows a double, and UTF_LENGTH^(-width) will underflow.
				 * Thus, in order to retain precision, we have to use BigDecimals with a high precision.
				 */
				BigDecimal startPoint = computeDecimalRepresentation(start);
				BigDecimal stopPoint = computeDecimalRepresentation(stop);
				BigDecimal elementPoint = computeDecimalRepresentation(element);

				BigDecimal run = stopPoint.subtract(startPoint);
				BigDecimal slope = BigDecimal.valueOf(totalElements).divide(run, slopeContext);

				BigDecimal estimate = slope.multiply(elementPoint.subtract(startPoint));
				return estimate.intValue();
		}

		private BigDecimal computeDecimalRepresentation(String string) {
				int length = string.length();
				BigDecimal sum = BigDecimal.ZERO;

				for(int offset =0,charPosition=0;offset<length;charPosition++){
						int codePoint = string.codePointAt(offset);
						int exp = width-charPosition;
						BigDecimal mult = BigDecimal.valueOf(codePoint).multiply(UTF_LENGTH.pow(exp));
						sum = sum.add(mult);
						offset+=Character.charCount(codePoint);
				}
				return sum;
		}

		public static void main(String...args) throws Exception{
				String[] data = new String[]{
								"a",
								"b",
								"c",
								"d",
								"aa",
								"aab",
								"aac",
								"aca",
								"bba",
								"bde",
								"cfd",
								"ddd",
								"ddddd"
				};
				EquiDepthUtf8Histogram hist = new EquiDepthUtf8Histogram(data,4,10);
				System.out.println(hist);
				System.out.println(hist.getNumElements("a","aac",true,false));
				System.out.println(hist.getNumElements("a","aab",true,false));
		}
}
