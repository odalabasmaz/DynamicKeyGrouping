package com.orhundalabasmaz.storm.loadBalancer.grouping.dkg;

import java.util.concurrent.TimeUnit;

/**
 * @author Orhun Dalabasmaz
 */
public class KeySpaceGC implements Runnable {
	private static final long TIME_INTERVAL = 60 * 10;
	private final KeySpace keySpace;

	public KeySpaceGC(KeySpace keySpace) {
		this.keySpace = keySpace;
	}

	@Override
	public void run() {
		while (true) {
			// garbage collecting: retiring
			System.out.println("######## garbage collecting");

			try {
				TimeUnit.SECONDS.sleep(TIME_INTERVAL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}