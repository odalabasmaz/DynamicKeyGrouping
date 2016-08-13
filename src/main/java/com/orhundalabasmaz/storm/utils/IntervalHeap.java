package com.orhundalabasmaz.storm.utils;

/*
 * File: IntervalHeap.java
 * Author: Keith Schwarz (htiek@cs.stanford.edu)
 *
 * An implementation of a double-ended priority queue
 * using an interval heap.  The interval heap is a special
 * data structure that, in some regards, can be thought of
 * as the superposition of a min-heap and a max-heap on
 * top of one another.  Each node stores a pair of two
 * values, which can be thought of as an "interval."
 * Insertion or deletion from an interval heap entails
 * inserting into either the min-heap or max-heap
 * appropriate for the inserted element.
 *
 * A good reference on interval heaps can be found at
 * http://www.cise.ufl.edu/~sahni/dsaaj/enrich/c13/double.htm
 */

import java.io.Serializable;
import java.util.*;

/**
 * A class representing an interval heap of a particular set of values.
 *
 * @param T The type of elements being stored.
 * @author Keith Schwarz (htiek@cs.stanford.edu)
 */
public final class IntervalHeap<T extends Comparable<T>>
		extends AbstractQueue<T>
		implements Serializable, Depque<T> {

	/* A Comparator for comparing elements in the heap. */
	private final Comparator<? super T> comparator;

	/**
	 * A utility Comparator which compares Comparable objects using
	 * their built-in compareTo functionality.
	 */
	private static final class DefaultComparator<T extends Comparable<T>> implements Comparator<T> {
		public int compare(T one, T two) {
			return one.compareTo(two);
		}
	}

	/**
	 * Each node in the interval heap stores two points defining the
	 * range.  In some cases, one of these points may not exist.  If
	 * this happens, we will represent it by storing the element in
	 * the 'low' slot and having 'high' be null.
	 *
	 * @param T The type of elements being stored.
	 */
	private static final class Node<T> implements Serializable {
		public T low;  // Low endpoint
		public T high; // High endpoint

		/**
		 * Creates a new Node with the specified initial values.
		 *
		 * @param low  The low value for the node.
		 * @param high The high value for the node.
		 */
		public Node(T low, T high) {
			this.low = low;
			this.high = high;
		}
	}

	/* We represent the interval heap using a compressed heap
	 * implementation that lays out the tree in sequential
	 * memory.  Each element is one-indexed, meaning that we
	 * have a dummy cell in the front.
	 */
	private final List<Node<T>> elems;

	private static final int DEFAULT_CAPACITY = 10;

	/* A cache of the number of elements in the heap. */
	private int numElems = 0;

	private transient int modCount = 0;

	/**
	 * Constructs a new IntervalHeap that is initially empty.
	 */
	public IntervalHeap() {
	    /* Use the default comparator. */
		this(DEFAULT_CAPACITY, new DefaultComparator<T>());
	}

	/**
	 * Constructs a new IntervalHeap that is initially empty and
	 * uses the specified comparator.
	 *
	 * @param comparator The comparator to use in the heap ordering.
	 */
	public IntervalHeap(Comparator<? super T> comparator) {
		this(DEFAULT_CAPACITY, comparator);
	}

	/*int initialCapacity*/
	public IntervalHeap(int initialCapacity) {
		this(initialCapacity, new DefaultComparator<T>());
	}

	public IntervalHeap(int initialCapacity, Comparator<? super T> comparator) {
		elems = new ArrayList<>(initialCapacity);

	    /* Cache the comparator for future use. */
		this.comparator = comparator;

        /* Add a dummy cell to the elements list to make all our
         * arithmetic 1-indexed.
         */
		elems.add(new Node<>(null, null));
	}

	/**
	 * Returns the number of elements in the IntervalHeap.
	 *
	 * @return The number of elements in the IntervalHeap.
	 */
	@Override
	public int size() {
		return numElems;
	}

	/**
	 * Returns whether the IntervalHeap is empty.
	 *
	 * @return Whether the IntervalHeap is empty.
	 */
	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Inserts a new element into the IntervalHeap.  The IntervalHeap
	 * does not support null elements.
	 *
	 * @param elem The element to insert.
	 * @throws NullPointerException If elem is null.
	 */
	private void addItem(T elem) {
	    /* Inserting into an IntervalHeap works by inserting into
	     * the last node (if possible), or adding a new singleton node
         * if need be.  Once that's done, it is inserted either into
         * the min-heap or max-heap as appropriate, completely ignoring
         * the other half of the heap.
         */
		if (elem == null)
			throw new NullPointerException("IntervalHeap does not store null values.");

        /* Determine whether the node should go in its own node, or in the unused space
         * in the singleton node at the end.
         */
		if (size() % 2 == 0) // New node
			elems.add(new Node<>(elem, null));
		else { // Unused space
			Node<T> currNode = elems.get(elems.size() - 1); // Last node in the tree

            /* Determine whether the value goes in the low or high slot based
             * on how it compares to the singleton.
             */
			if (comparator.compare(currNode.low, elem) > 0) { // Goes in low slot
			    /* Move the singleton to the high slot. */
				currNode.high = currNode.low;
				currNode.low = elem;
			} else // Goes in high slot
				currNode.high = elem;
		}

        /* Bump the element count to ensure we track size correctly. */
		++numElems;

        /* Determine whether to do a min-heap or max-heap insert.  However, we can
         * only decide this if there's a parent node, since otherwise there's nothing
         * to compare against.
         */
		if (size() <= 2)
			return;

        /* If the node is less than the low element of its parent, do a min-heap
         * insert since it can't exceed the parent's upper-bound.
         */
		Node<T> parent = elems.get((elems.size() - 1) / 2);
		if (comparator.compare(parent.low, elem) > 0)
			minHeapInsert();
	    /* Otherwise, if it's bigger than the high element of its parent, do a
         * max-heap insert since it can't be lower than the parent's
         * lower-bound.
         */
		else if (comparator.compare(parent.high, elem) < 0)
			maxHeapInsert();
        /* Otherwise, the node is in the right place. */
	}

	/**
	 * Utility function to perform a min-heap insert.  This function
	 * assumes that the element to bubble up is in the final slot of
	 * the tree.
	 */
	private void minHeapInsert() {
		int index = elems.size() - 1;
		Node<T> currNode = elems.get(index);

        /* Keep bubbling up until we hit the root or are in the right place. */
		while (index > 1) {
            /* Look up the parent. */
			int parentIndex = index / 2;
			Node<T> parentNode = elems.get(parentIndex);

            /* If we're above the lower bound, we're done. */
			if (comparator.compare(currNode.low, parentNode.low) >= 0) break;

            /* Otherwise, swap with the parent and repeat. */
			T temp = currNode.low;
			currNode.low = parentNode.low;
			parentNode.low = temp;

            /* Update the index and position to reflect the change. */
			index = parentIndex;
			currNode = parentNode;
		}
	}

	/**
	 * Utility function to perform a max-heap insert.  This function
	 * is a bit more complex than the previous one because we need to
	 * handle the case where the node is a singleton.
	 */
	private void maxHeapInsert() {
		int index = elems.size() - 1;
		Node<T> currNode = elems.get(index);

        /* Keep bubbling up until we hit the root or are in the right place. */
		while (index > 1) {
            /* Look up the parent. */
			int parentIndex = index / 2;
			Node<T> parentNode = elems.get(parentIndex);

            /* Tricky edge case!  If this is the very last node and a singleton, we want
             * to compare the low field of the node rather than the high field.
             */
			if (currNode.high == null) { // Singleton
                /* If we're below the upper bound, we're done. */
				if (comparator.compare(currNode.low, parentNode.high) < 0) break;

                /* Otherwise, swap with the parent and repeat. */
				T temp = currNode.low;
				currNode.low = parentNode.high;
				parentNode.high = temp;

                /* Update the index and position to reflect the change. */
				index = parentIndex;
				currNode = parentNode;
			} else { // Doubleton
                /* If we're below the lower bound, we're done. */
				if (comparator.compare(currNode.high, parentNode.high) < 0) break;

                /* Otherwise, swap with the parent and repeat. */
				T temp = currNode.high;
				currNode.high = parentNode.high;
				parentNode.high = temp;

                /* Update the index and position to reflect the change. */
				index = parentIndex;
				currNode = parentNode;
			}
		}
	}

	/**
	 * Returns (but does not remove) the minimum element of the heap.  If the
	 * heap is empty, this method throws a NoSuchElementException.
	 *
	 * @return The minimum element of the heap.
	 * @throws NoSuchElementException If the heap is empty.
	 */
	private T min() {
        /* Check whether we're empty. */
		if (isEmpty())
			throw new NoSuchElementException("Empty heap.");

        /* The minimum element is always in the 'low' slot of the topmost node. */
		return elems.get(1).low; // Array is one-indexed
	}

	/**
	 * Returns (but does not remove) the maximum element of the heap.  If the
	 * heap is empty, this method throws a NoSuchElementException.
	 *
	 * @return The maximum element of the heap.
	 * @throws NoSuchElementException If the heap is empty.
	 */
	private T max() {
        /* Check whether we're empty. */
		if (isEmpty())
			throw new NoSuchElementException("Empty heap.");

        /* There are two cases:
         * 1. If there is exactly one element in the heap, then it would be in the
         *    "low" slot of the heap node.
         * 2. Otherwise, the max element is in the "high" slot of the topmost
         *    node.
         */
		return size() == 1 ? elems.get(1).low : elems.get(1).high; // Array is one-indexed.
	}

	/**
	 * Removes and returns the minimum element of the heap.  If the heap is
	 * empty, this method throws a NoSuchElementException.
	 *
	 * @return The smallest element of the heap.
	 * @throws NoSuchElementException If the heap is empty.
	 */
	public T dequeueMin() {
        /* Cache the value to return; this also checks for an empty heap. */
		T toReturn = min();

        /* If this is a singleton heap, throw out the last node.  We're done. */
		if (size() == 1) {
			elems.remove(1);
			--numElems;
			return toReturn;
		}

        /* Move the min element from the last node to fill the
         * place of the element we just removed.  This might
         * empty the last node, in which case we remove it.
         */
		Node<T> lastNode = elems.get(elems.size() - 1);
		elems.get(1).low = lastNode.low;

        /* Odd number of elements; remove the last node. */
		if (size() % 2 == 1)
			elems.remove(elems.size() - 1);
        /* Otherwise, scoot the high element down to the
         * low slot.
         */
		else {
			lastNode.low = lastNode.high;
			lastNode.high = null;
		}
		--numElems;

        /* Continously do a bubble-down, at each point ensuring that the
         * endpoints of the current node are correct.
         */
		int index = 1;
		Node<T> currNode = elems.get(index);

		while (true) {
            /* If we have no children, we're done. */
			if (index * 2 >= elems.size())
				break;

            /* Otherwise, we either have one child or two children.  Check which case we're in. */
			int childToCompareTo; // Which child we'll end up testing

            /* If we have two children, compare the two and store the smaller one. */
			if (index * 2 + 1 < elems.size())
				childToCompareTo = (comparator.compare(elems.get(index * 2).low, elems.get(2 * index + 1).low) < 0 ? index * 2 : index * 2 + 1);
            /* Otherwise, only compare to the one child we have. */
			else
				childToCompareTo = index * 2;

            /* If we are smaller than the child, we're done. */
			Node<T> child = elems.get(childToCompareTo);
			if (comparator.compare(currNode.low, child.low) < 0)
				break;

            /* Otherwise, swap down and continue. */
			T temp = child.low;
			child.low = currNode.low;
			currNode.low = temp;

            /* Check that the child's endpoints are ordered correctly.  When doing
             * so, check that the high field isn't null, since if we hit the very
             * last node we don't want to compare against null.
             */
			if (child.high != null && comparator.compare(child.low, child.high) > 0) {
                /* Swap the two. */
				temp = child.low;
				child.low = child.high;
				child.high = temp;
			}

            /* Update position and node. */
			index = childToCompareTo;
			currNode = child;
		}

        /* All done!  Return the proper value. */
		return toReturn;
	}

	/**
	 * Removes and returns the maximum element of the heap.  If the heap is
	 * empty, this method throws a NoSuchElementException.
	 *
	 * @return The largest element of the heap.
	 * @throws NoSuchElementException If the heap is empty.
	 */
	public T dequeueMax() {
        /* Cache the value to return; this also checks for an empty
         * heap.
         */
		T toReturn = max();

        /* If this is a singleton heap, throw out the node and return. */
		if (size() == 1) {
			elems.remove(1);
			--numElems;
			return toReturn;
		}

        /* Move the max element from the last node to fill the
         * place of the element we just removed.  The logic
         * here is a bit tricky because the max element in that
         * node might actually be in the low slot if there are
         * an odd number of elements in the heap.
         */
		Node<T> lastNode = elems.get(elems.size() - 1);
		if (size() % 2 == 1) {
            /* Grab from the low field and throw the last node away. */
			elems.get(1).high = lastNode.low;
			elems.remove(elems.size() - 1);
		} else {
            /* Grab from the high field, then clear it. */
			elems.get(1).high = lastNode.high;
			lastNode.high = null;
		}
		--numElems;

        /* Continously do a bubble-down, at each point ensuring that the
         * endpoints of the current node are correct.
         */
		int index = 1;
		Node<T> currNode = elems.get(index);

		while (true) {
            /* If we have no children, we're done. */
			if (index * 2 >= elems.size())
				break;

            /* Otherwise, we either have one child or two children.  Check which case we're in. */
			int childToCompareTo; // Which child we'll end up testing

            /* If we have two children, compare the two and store the smaller one. */
			if (index * 2 + 1 < elems.size()) {
                /* Tricky case - if the second child is the very last node and there are an odd number
                 * of elements, compare the low of the last node and the high of the other child.
                 * Otherwise, compare their high fields.
                 */
				if (size() % 2 == 1 && index * 2 + 1 == elems.size() - 1)
					childToCompareTo = (comparator.compare(elems.get(index * 2).high, elems.get(2 * index + 1).low) > 0 ? index * 2 : index * 2 + 1);
				else
					childToCompareTo = (comparator.compare(elems.get(index * 2).high, elems.get(2 * index + 1).high) > 0 ? index * 2 : index * 2 + 1);
			}
            /* Otherwise, only compare to the one child we have. */
			else
				childToCompareTo = index * 2;

            /* Determine our relation to the child.  If the child is the odd case, make sure not
             * to read its high field!
             */
			Node<T> child = elems.get(childToCompareTo);
			if (child.high == null) {
                /* See if we're done, and swap if we're not. */
				if (comparator.compare(child.low, currNode.high) < 0)
					break;

                /* Swap the element down. */
				T temp = child.low;
				child.low = currNode.high;
				currNode.high = temp;
			}
            /* Not in the edge case, so just check if this node is bigger than its biggest child. */
			else {
				if (comparator.compare(child.high, currNode.high) < 0)
					break;

                /* Otherwise, swap down to the child node */
				T temp = child.high;
				child.high = currNode.high;
				currNode.high = temp;

                /* Finally, if the child's nodes are out of order, fix them. */
				if (comparator.compare(child.low, child.high) > 0) {
					temp = child.high;
					child.high = child.low;
					child.low = temp;
				}
			}

            /* Update position and node. */
			index = childToCompareTo;
			currNode = child;
		}

        /* All done!  Return the proper value. */
		return toReturn;
	}

	/**
	 * double ended priority queue interface implementations
	 */
	@Override
	public boolean offer(T item) {
		addItem(item);
		modCount++;
		return true;
	}

	@Override
	public T peekFirst() {
		return isEmpty() ? null : elems.get(1).low;
	}

	@Override
	public T peekLast() {
		return isEmpty() ? null : size() == 1 ? elems.get(1).low : elems.get(1).high;
	}

	@Override
	public T pollFirst() {
		modCount++;
		return isEmpty() ? null : dequeueMin();
	}

	@Override
	public T pollLast() {
		modCount++;
		return isEmpty() ? null : dequeueMax();
	}

	@Override
	public T poll() {
		return pollFirst();
	}

	@Override
	public T peek() {
		return peekFirst();
	}

	@Override
	public void clear() {
		super.clear();
		modCount++;
	}

	@Override
	public Iterator<T> iterator() {
		return new Itr();
	}

	private final class Itr implements Iterator<T> {
		private int cursor = 0;
		private int leftCursor = 1;
		private int rightCursor = 1;
		private int expectedModCount = modCount;

		@Override
		public boolean hasNext() {
			return cursor != size();
		}

		@Override
		public T next() {
			checkForConcurrentModification();
			T item;
			if (leftCursor == rightCursor) {
				item = elems.get(leftCursor).low;
				leftCursor++;
			} else if (leftCursor > rightCursor) {
				item = elems.get(rightCursor).high;
				rightCursor++;
			} else {
				throw new IllegalStateException();
			}
			++cursor;
			return item;
		}

		@Override
		public void remove() {
			checkForConcurrentModification();

		}

		private void checkForConcurrentModification() {
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}
		}
	}
}
