/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.speculativeframe;

import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class DoubleLinkedListTest {
    @Test
    public void testGetAndAdd() {
        DoubleLinkedList<DoubleLinkedListNode<String>> list = new DoubleLinkedList<>();
        list.addLast(new DoubleLinkedListNode<>("1"));
        list.addLast(new DoubleLinkedListNode<>("2"));
        list.addLast(new DoubleLinkedListNode<>("3"));

        assertEquals(list.getFirst().getPayload(), "1");
        assertEquals(list.getFirst().next.getPayload(), "2");
        assertEquals(list.getFirst().next.next.getPayload(), "3");
        assertEquals(list.getLast().getPayload(), "3");
        assertEquals(list.getLast().prev.getPayload(), "2");
        assertEquals(list.getLast().prev.prev.getPayload(), "1");

        Iterator<DoubleLinkedListNode<String>> iterator = list.iterator();

        assertEquals(iterator.next().getPayload(), "1");
        assertEquals(iterator.next().getPayload(), "2");
        assertEquals(iterator.next().getPayload(), "3");
    }


    @Test
    public void testMultiThreadAdd() throws ExecutionException, InterruptedException {
        DoubleLinkedList<DoubleLinkedListNode<String>> list = new DoubleLinkedList<>();
        CompletableFuture<Void> future1 = CompletableFuture.supplyAsync(
                () -> {
                    list.addFirst(new DoubleLinkedListNode<>("1"));
                    return null;
                });

        CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(
                () -> {
                    list.addFirst(new DoubleLinkedListNode<>("2"));
                    return null;
                });

        future1.join();
        future2.join();

        assertEquals(2, list.size());

        DoubleLinkedListNode<String> first = list.getFirst();
        assertSame(first.next, list.getLast());
        assertSame(first, list.getLast().prev);
    }

    @Test
    public void testMultiThreadUpdate() throws ExecutionException, InterruptedException {
        DoubleLinkedList<DoubleLinkedListNode<String>> list = new DoubleLinkedList<>();
        list.addFirst(new DoubleLinkedListNode<>("1"));
        list.addFirst(new DoubleLinkedListNode<>("2"));
        list.addFirst(new DoubleLinkedListNode<>("3"));
        CompletableFuture<Void> future1 = CompletableFuture.supplyAsync(
                () -> {
                    list.makeFirst(new DoubleLinkedListNode<>("2"));
                    return null;
                });

        CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(
                () -> {
                    list.makeFirst(new DoubleLinkedListNode<>("3"));
                    return null;
                });
        future1.join();
        future2.join();
        assertEquals(3, list.size());

        DoubleLinkedListNode<String> first = list.getFirst();
        if (first.getPayload().equals("2")) {
            // 2->3->1
            assertEquals("3", first.next.getPayload());
        } else if(first.getPayload().equals("3")) {
            // 3->2->1
            assertEquals("2", first.next.getPayload());
        } else {
            fail();
        }
    }
}
