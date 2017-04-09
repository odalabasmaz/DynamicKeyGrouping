package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.utils.DKGUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

/**
 * @author Orhun Dalabasmaz
 */
public class ProcessorTest {
	private int currCycle;

	@Test
	public void process() {
		int count = 10_000_000;
		int cycle = 1000;
		int sleep = 1;
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
//		doJob(count, sleep);
		doCycleJob(count, sleep, cycle);
		stopWatch.stop();
		long duration = stopWatch.getTime();
		System.out.printf("record: %s, duration: %d ms\n", count, duration);
	}

	private void doJob(int count, int sleep) {
		/*todo: sleep methodu n kadar dönsün sonra thread.sleep yapsın */
		for (int i = 0; i < count; ++i) {
			try {
				Thread.sleep(0, 1);
			} catch (InterruptedException e) {
				System.err.println("dafaf");
			}
//			DKGUtils.sleepInNanoseconds(sleep);
//			DKGUtils.sleepInMicroseconds(sleep);
		}
	}

	private void doCycleJob(int count, int sleep, int cycle) {
		for (int i = 0; i < count; ++i) {
			++currCycle;
			if (currCycle / cycle > 0) {
				DKGUtils.sleepInMilliseconds(sleep);
				currCycle = 0;
			}
		}
	}
}
