package com.orhundalabasmaz.storm.utils;

/**
 * @author Orhun Dalabasmaz
 */
public class Statistics {

	private Statistics() {
	}

	public static double standardDeviation(double[] values) {
		if (values == null || values.length == 0) {
			return -1;
		}

		double avg = 0;
		for (double v : values) {
			avg += v;
		}
		avg /= values.length;

		double val = 0;
		for (double v : values) {
			val += Math.pow(v - avg, 2);
		}
		return Math.sqrt(val / values.length);
	}
}
