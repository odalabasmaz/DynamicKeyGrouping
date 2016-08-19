package com.orhundalabasmaz.storm.loadBalancer.grouping.dkg;

import com.orhundalabasmaz.storm.utils.DKGUtils;

/**
 * @author Orhun Dalabasmaz
 */
public class KeySpaceManager implements Runnable {
	private static final long TIME_INTERVAL = 10;
	private static final long CYCLE_COUNT = 6;
	private final KeySpace keySpace;

	public KeySpaceManager(KeySpace keySpace) {
		this.keySpace = keySpace;
	}

	@Override
	public void run() {
		int count = 0;
		while (true) {
			synchronized (keySpace) {
				++count;

				// rearrange keys in space
//				System.out.println("######## rearranging keys > babySpace to teenageSpace, count: " + count);
				keySpace.sortBabySpace();
				keySpace.truncateBabySpace();
				keySpace.sortTeenageSpace();
				keySpace.upToTeenageSpace();

				if (count == CYCLE_COUNT) {
					count = 0;
//					System.out.println("######## rearranging keys > teenage space to old space");
					keySpace.sortTeenageSpace();
					keySpace.sortOldSpace();
					keySpace.upToOldSpace();
				}
			}

			// wait for next execution
			DKGUtils.sleepInSeconds(TIME_INTERVAL);
		}
	}
}
