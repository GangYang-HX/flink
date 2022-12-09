package org.apache.flink.taishan.state.cache;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LRUCache {

	@VisibleForTesting
	public LinkedNodeList list;

	@VisibleForTesting
	public ConcurrentMap<ByteArrayWrapper, Node> map;
	int cap;

	long expireTime;

	// Minimum check duration for cache clean. Time Unit: Milliseconds
	private static final long MIN_CHECK_DURATION = 60 * 1000;

	long lastCleanTime;

	private static final Logger LOG = LoggerFactory.getLogger(LRUCache.class);

	public LRUCache(int capacity, long expireAfter) {
		this.list = new LinkedNodeList();
		this.map = new ConcurrentHashMap<>(capacity);
		if (capacity <= 0) {
			throw new FlinkRuntimeException("cache capacity cannot be null");
		}
		this.cap = capacity;
		this.lastCleanTime = System.currentTimeMillis();
		this.expireTime = Math.min(MIN_CHECK_DURATION, expireAfter / 2);
	}

	public CacheEntry get(ByteArrayWrapper key) {
		Node node = map.get(key);
		if (node == null) {
			return null;
		}

		if (node.val != null && node.val.getTimeStamp() + expireTime < System.currentTimeMillis()) {
			return null;
		}

		list.moveToHead(node);
		return node.val;
	}

	public void delete(ByteArrayWrapper key) {
		Node node = map.get(key);
		if (node != null) {
			map.remove(node.key);
			list.remove(node);
		}
	}
	@VisibleForTesting
	public void markAsPersisted(ByteArrayWrapper key) {
		Node node = map.get(key);
		if (node != null) {
			node.val.setAction(CacheEntry.DONE_ACTION);
		}
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public void put(ByteArrayWrapper key, CacheEntry value) {
		Node node = map.get(key);
		if (node != null) {
			list.moveToHead(node);
			node.val = value;
		} else {
			Node newNode = new Node(key, value);
			map.put(key, newNode);
			list.addToHead(newNode);
		}

		long now = System.currentTimeMillis();
		if (map.size() >= cap || lastCleanTime + (expireTime / 4) < now) {
			int evictedSize = cleanEviction();
			if (evictedSize > 0) {
				LOG.info("Evicted {} entries in cache", evictedSize);
			}

			if (map.size() >= cap) {
				cap = map.size();
				LOG.info("Failed to evict caches when cache size reaches capacity. Increasing capacity to {}.", cap);
			}
			lastCleanTime = now;
		}
	}

	public int size() {
		int size = 0;
		for (Map.Entry<ByteArrayWrapper, Node> entry : map.entrySet()) {
			if (entry.getValue().val.getDataObject() != null) {
				size++;
			}
		}
		return size;
	}

	public Map<ByteArrayWrapper, Node> asMap() {
		return map;
	}

	@VisibleForTesting
	public int cleanEviction() {
		Node node = list.dummyTail.prev;
		long now = System.currentTimeMillis();
		int count = 0;
		while (node != list.dummyHead) {
			if (node.val.getAction() == CacheEntry.DONE_ACTION
				&& node.val.getTimeStamp() + expireTime < now) {
				delete(node.key);
				count++;
			}
			node = node.prev;
		}
		return count;
	}
}

class LinkedNodeList {
	public Node dummyHead;
	public Node dummyTail;

	LinkedNodeList() {
		dummyHead = new Node(null, null);
		dummyTail = new Node(null, null);
		dummyHead.next = dummyTail;
		dummyTail.prev = dummyHead;
	}

	void moveToHead(Node node) {
		node.prev.next = node.next;
		node.next.prev = node.prev;
		addToHead(node);
	}

	void addToHead(Node node) {
		Node tmp = dummyHead.next;
		dummyHead.next = node;
		node.next = tmp;
		node.prev = dummyHead;
		tmp.prev = node;
		node.val.setTimeStamp(System.currentTimeMillis());
	}

	void removeTail() {
		Node newTail = dummyTail.prev.prev;
		newTail.next = dummyTail;
		dummyTail.prev = newTail;
	}

	void removeHead() {
		Node newHead = dummyHead.next.next;
		newHead.prev = dummyHead;
		dummyHead.next = newHead;
	}

	void remove(Node node) {
		if (node == dummyHead.next) {
			removeHead();
		} else if (node == dummyTail.prev) {
			removeTail();
		} else {
			Node newPrev = node.prev;
			Node newNext = node.next;
			newPrev.next = newNext;
			newNext.prev = newPrev;
		}
	}

	@VisibleForTesting
	public int linkedListSize() {
		int size = 0;
		Node node = dummyTail.prev;
		while (node != dummyHead) {
			node = node.prev;
			size++;
		}

		return size;
	}
}

