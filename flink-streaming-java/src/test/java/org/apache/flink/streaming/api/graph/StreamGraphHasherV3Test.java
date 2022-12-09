package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StreamGraphHasherV3Test {

    // 6 data streams + 2 partitioners
    private static final int NUM_TRANSFORMATIONS = 10;

    @Test
    public void testHashTwice() {
        Tuple2<StreamExecutionEnvironment, List<Transformation<?>>> envAndTransformations =
                generateTransformations(true);

        Map<Integer, byte[]> hashes = generateHashes(envAndTransformations.f0, envAndTransformations.f1);

        Tuple2<StreamExecutionEnvironment, List<Transformation<?>>> secondEnvAndTransformations =
                generateTransformations(true);

        Map<Integer, byte[]> secondHashes = generateHashes(
                secondEnvAndTransformations.f0, secondEnvAndTransformations.f1);

        for (Integer id : hashes.keySet()) {
            byte[] hash = hashes.get(id);
            Assert.assertTrue(secondHashes.containsKey(id + NUM_TRANSFORMATIONS));

            byte[] secondHash = secondHashes.get(id + NUM_TRANSFORMATIONS);
            Assert.assertArrayEquals(hash, secondHash);
        }
    }

    @Test
    public void testWhenPartitionerChanges() {
        Tuple2<StreamExecutionEnvironment, List<Transformation<?>>> envAndTransformations =
                generateTransformations(true);

        Map<Integer, byte[]> hashes = generateHashes(envAndTransformations.f0, envAndTransformations.f1);

        Tuple2<StreamExecutionEnvironment, List<Transformation<?>>> secondEnvAndTransformations =
                generateTransformations(false);

        Map<Integer, byte[]> secondHashes = generateHashes(
                secondEnvAndTransformations.f0, secondEnvAndTransformations.f1);

        for (Integer id : hashes.keySet()) {
            byte[] hash = hashes.get(id);
            Assert.assertTrue(secondHashes.containsKey(id + NUM_TRANSFORMATIONS));

            byte[] secondHash = secondHashes.get(id + NUM_TRANSFORMATIONS);
            Assert.assertArrayEquals(secondHash, hash);
        }
    }

    private Tuple2<StreamExecutionEnvironment, List<Transformation<?>>>generateTransformations(
            boolean useRescalePartitioner) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);
        DataStream<Integer> mapDataStream;
        if (!useRescalePartitioner) {
            mapDataStream = sourceDataStream.rebalance().map(x -> x + 1);
        } else {
            mapDataStream = sourceDataStream.rescale().map(x -> x + 1);
        }

        DataStream<Integer> sourceDataStream1 = env.fromElements(4, 5, 6);
        DataStream<Integer> mapDataStream1;
        if (!useRescalePartitioner) {
            mapDataStream1 = sourceDataStream1.rebalance().map(x -> x + 1);
        } else {
            mapDataStream1 = sourceDataStream1.rescale().map(x -> x + 1);
        }

        DataStream<Integer> unionDataStream = mapDataStream.union(mapDataStream1);
        DataStreamSink<Integer> printDataStream = unionDataStream.print();

        final List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceDataStream.getTransformation());
        transformations.add(mapDataStream.getTransformation());
        transformations.add(sourceDataStream1.getTransformation());
        transformations.add(mapDataStream1.getTransformation());
        transformations.add(unionDataStream.getTransformation());
        transformations.add(printDataStream.getTransformation());

        return new Tuple2<>(env, transformations);
    }

    private Map<Integer, byte[]> generateHashes(
            StreamExecutionEnvironment streamEnv,
            List<Transformation<?>> transformations) {
        StreamGraph streamGraph = new StreamGraphGenerator(
                transformations,
                streamEnv.getConfig(),
                streamEnv.getCheckpointConfig())
                .generate();

        StreamGraphHasherV3 hasher = new StreamGraphHasherV3();
        return hasher.traverseStreamGraphAndGenerateHashes(streamGraph);
    }
}
