package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import com.orhundalabasmaz.storm.utils.CustomLogger;
import com.orhundalabasmaz.storm.utils.DKGUtils;

/**
 * @author Orhun Dalabasmaz
 */
public class KeySpaceGC implements Runnable {
	private static final long TIME_INTERVAL = 1 * 60L;
	private final KeySpace keySpace;

	public KeySpaceGC(KeySpace keySpace) {
		this.keySpace = keySpace;
	}

	@Override
	public void run() {
		while (true) {
			// garbage collecting: retiring
			CustomLogger.info("######## garbage collecting");
			DKGUtils.sleepInSeconds(TIME_INTERVAL);
		}
	}
}