package org.apache.flink.bilibili.udf.scalar.location;


public class NotFoundException extends Exception {

    public NotFoundException(String name) {
        super(name);
    }
}
