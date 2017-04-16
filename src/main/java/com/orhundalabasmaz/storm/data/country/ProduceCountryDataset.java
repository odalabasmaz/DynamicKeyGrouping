package com.orhundalabasmaz.storm.data.country;

/**
 * @author Orhun Dalabasmaz
 */
public class ProduceCountryDataset {

	private ProduceCountryDataset() {
	}

	public static void main(String... args) {
		Producer producer;
		String fileNamePrefix;

		// balanced skewness
		fileNamePrefix = "country-skew-r";
		for (int ratio = 0; ratio <= 100; ratio += 10) {
			producer = new Producer(fileNamePrefix, ratio);
			producer.produce();
		}

		// unbalanced skewness
		fileNamePrefix = "country-half-skew-r";
		producer = new Producer(fileNamePrefix, 80);
		producer.produceHalfSkew();
	}
}
