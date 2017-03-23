package com.orhundalabasmaz.storm.data;

import com.orhundalabasmaz.storm.data.configuration.StreamReactor;

/**
 * @author Orhun Dalabasmaz
 */
public class ConfigurationGenerator {

	private ConfigurationGenerator() {
	}

	/**
	 * */
	public static void main(String[] args) {
		StreamReactor reactor = new StreamReactor();
		reactor.generateFiles();
		reactor.generateScripts();
	}
}
