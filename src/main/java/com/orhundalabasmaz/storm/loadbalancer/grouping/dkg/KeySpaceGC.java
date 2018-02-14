package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Orhun Dalabasmaz
 */
public class KeySpaceGC implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(KeySpaceGC.class);
	private static final long TIME_INTERVAL = 1 * 60L;
	private boolean run = true;
	private final KeySpace keySpace;

	public KeySpaceGC(KeySpace keySpace) {
		this.keySpace = keySpace;
	}

	@Override
	public void run() {
		while (run) {
			// garbage collecting: retiring
			LOGGER.info("garbage collecting");

			//todo: iterate KeyItems in TeenSpace & OldSpace and check latestUpdateTime then delete if not seen for a long time
			//or we may put it into teenage space
			synchronized (keySpace) {
				keySpace.gc();
			}

			DKGUtils.sleepInSeconds(TIME_INTERVAL);
		}
	}

	public void terminate() {
		run = false;
	}
}