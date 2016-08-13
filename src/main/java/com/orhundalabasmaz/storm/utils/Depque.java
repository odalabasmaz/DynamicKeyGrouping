package com.orhundalabasmaz.storm.utils;

import java.util.Queue;

/**
 * @author Orhun Dalabasmaz
 */
public interface Depque<E> extends Queue<E> {
	int size();

	boolean isEmpty();

	boolean offer(E item);

	E peekFirst();

	E peekLast();

	E pollFirst();

	E pollLast();
}
