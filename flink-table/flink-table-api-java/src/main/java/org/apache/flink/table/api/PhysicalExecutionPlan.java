package org.apache.flink.table.api;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className PhysicalExecutionPlan.java
 * @description This is the description of PhysicalExecutionPlan.java
 * @createTime 2021-01-11 19:14:00
 */
public class PhysicalExecutionPlan {
    private String operatorName;
    private Integer id;
    private String nodeType;
    private String content;
    private String shipStrategy;

    public PhysicalExecutionPlan() {
    }

    public String getOperatorName() {
        return operatorName;
    }

    public Integer getId() {
        return id;
    }

    public String getNodeType() {
        return nodeType;
    }

    public String getContent() {
        return content;
    }

    public String getShipStrategy() {
        return shipStrategy;
    }

    public static final class PhysicalExecutionPlanBuilder {
        private String operatorName;
        private Integer id;
        private String nodeType;
        private String content;
        private String shipStrategy;

        private PhysicalExecutionPlanBuilder() {
        }

        public static PhysicalExecutionPlanBuilder builder() {
            return new PhysicalExecutionPlanBuilder();
        }

        public PhysicalExecutionPlanBuilder withOperatorName(String operatorName) {
            this.operatorName = operatorName;
            return this;
        }

        public PhysicalExecutionPlanBuilder withId(Integer id) {
            this.id = id;
            return this;
        }

        public PhysicalExecutionPlanBuilder withNodeType(String nodeType) {
            this.nodeType = nodeType;
            return this;
        }

        public PhysicalExecutionPlanBuilder withContent(String content) {
            this.content = content;
            return this;
        }

        public PhysicalExecutionPlanBuilder withShipStrategy(String shipStrategy) {
            this.shipStrategy = shipStrategy;
            return this;
        }

        public PhysicalExecutionPlan build() {
            PhysicalExecutionPlan physicalExecutionPlan = new PhysicalExecutionPlan();
            physicalExecutionPlan.nodeType = this.nodeType;
            physicalExecutionPlan.operatorName = this.operatorName;
            physicalExecutionPlan.id = this.id;
            physicalExecutionPlan.shipStrategy = this.shipStrategy;
            physicalExecutionPlan.content = this.content;
            return physicalExecutionPlan;
        }
    }

    @Override
    public String toString() {
        return "PhysicalExecutionPlan{" +
                "operatorName='" + operatorName + '\'' +
                ", id=" + id +
                ", nodeType='" + nodeType + '\'' +
                ", content='" + content + '\'' +
                ", shipStrategy='" + shipStrategy + '\'' +
                '}';
    }
}
