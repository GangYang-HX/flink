package com.bilibili.bsql.kafka.diversion.udf;

import org.apache.flink.table.functions.ScalarFunction;

/** BrokerUdf. */
public class BrokerUdf extends ScalarFunction {

    public String eval(String brokerArg) throws Exception {
        //        switch (brokerArg) {
        //            case "cluster1":
        //                return "10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092";
        //            case "cluster2":
        //                return "10.221.51.174:9092";
        //            default:
        //                throw new Exception("error");
        //        }
        return "10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092";
    }
}
