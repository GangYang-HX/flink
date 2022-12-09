package org.apache.flink.taishan.state.cache;


public class Node {
	public ByteArrayWrapper key;
	public CacheEntry val;
	Node next;
	Node prev;

	Node(ByteArrayWrapper key, CacheEntry value) {
		this.key = key;
		this.val = value;
	}
}
