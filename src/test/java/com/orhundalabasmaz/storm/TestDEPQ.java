package com.orhundalabasmaz.storm;

import com.orhundalabasmaz.storm.loadBalancer.grouping.dkg.KeyItem;
import com.orhundalabasmaz.storm.utils.Depque;
import com.orhundalabasmaz.storm.utils.IntervalHeap;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * @author Orhun Dalabasmaz
 */
public class TestDEPQ {

	@Test
	public void depq() {
		Depque<Integer> depq = new IntervalHeap<>((n1, n2) -> (n2 - n1));

		depq.offer(4);
		depq.offer(18);
		depq.offer(12);
		depq.offer(20);
		depq.offer(1);
		depq.offer(10);
		depq.offer(2);
		depq.offer(14);
		depq.offer(700);

		for (Integer i : depq) {
			System.out.printf(i + " ");
		}

		assertTrue(depq.pollFirst() == 700);
		assertTrue(depq.pollLast() == 1);

		assertTrue(depq.pollFirst() == 20);
		assertTrue(depq.pollLast() == 2);
	}

	@Test
	public void priorityQueue() {
		Queue<Integer> queue = new PriorityQueue<>((n1, n2) -> (n1 - n2));
		queue.offer(4);
		queue.offer(2);
		queue.offer(6);
		queue.offer(1);
		queue.offer(3);
		queue.offer(7);

		assertTrue(queue.poll() == 1);
		assertTrue(queue.poll() == 2);
		assertTrue(queue.poll() == 3);

//		assertTrue(queue.poll() == 7);
//		assertTrue(queue.poll() == 6);
//		assertTrue(queue.poll() == 4);
	}

	@Test
	public void depqKeyItem() {
		Depque<KeyItem> teenageSpace = new IntervalHeap<>(20, (Comparator<KeyItem> & Serializable) (n1, n2) -> (int) (n1.getCount() - n2.getCount()));
		KeyItem item1 = new KeyItem("item1");
		item1.appearedAgain();
		item1.appearedAgain();
		teenageSpace.offer(item1);

		KeyItem item2 = new KeyItem("item2");
		item2.appearedAgain();
		item2.appearedAgain();
		item2.appearedAgain();
		teenageSpace.offer(item2);

		KeyItem item3 = new KeyItem("item3");
		item3.appearedAgain();
		teenageSpace.offer(item3);

		KeyItem item4 = new KeyItem("item4");
		item4.appearedAgain();
		item4.appearedAgain();
		item4.appearedAgain();
		item4.appearedAgain();
		teenageSpace.offer(item4);

		for (KeyItem i : teenageSpace) {
			System.out.printf(i.getKey() + " - " + i.getCount() + " ");
		}
		System.out.println();

		item3.appearedAgain();
		item3.appearedAgain();
		item3.appearedAgain();
		item3.appearedAgain();

		for (KeyItem i : teenageSpace) {
			System.out.printf(i.getKey() + " - " + i.getCount() + " ");
		}
		System.out.println();

		if (teenageSpace.remove(item3))
			teenageSpace.offer(item3);
		else
			System.out.println("problem!");


		for (KeyItem i : teenageSpace) {
			System.out.printf(i.getKey() + " - " + i.getCount() + " ");
		}
		System.out.println();
	}
}
