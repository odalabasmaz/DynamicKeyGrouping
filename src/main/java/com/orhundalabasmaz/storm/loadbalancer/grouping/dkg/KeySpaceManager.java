package com.orhundalabasmaz.storm.loadbalancer.grouping.dkg;

import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Orhun Dalabasmaz
 */
public class KeySpaceManager implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(KeySpaceManager.class);
	private static final long TIME_INTERVAL = 15;
	private static final long CYCLE_COUNT = 4;
	private boolean run = true;
	private final KeySpace keySpace;

	public KeySpaceManager(KeySpace keySpace) {
		this.keySpace = keySpace;
	}

	@Override
	public void run() {
		int count = 0;
		while (run) {
			++count;

			synchronized (keySpace) {
				// rearrange keys in space
				LOGGER.info("rearranging keys > babySpace to teenageSpace, count: {}", count);
				keySpace.sortBabySpace();
				keySpace.truncateBabySpace();
				keySpace.sortTeenageSpace();
				keySpace.upToTeenageSpace();

				if (count == CYCLE_COUNT) {
					count = 0;
					LOGGER.info("rearranging keys > teenage space to old space");
					keySpace.sortTeenageSpace();
					keySpace.sortOldSpace();
					keySpace.upToOldSpace();
				}
			}

			// wait for next execution
			DKGUtils.sleepInSeconds(TIME_INTERVAL);
		}
	}

	public void terminate() {
		run = false;
	}
}
