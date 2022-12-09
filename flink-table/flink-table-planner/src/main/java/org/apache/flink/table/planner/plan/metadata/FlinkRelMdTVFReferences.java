package org.apache.flink.table.planner.plan.metadata;

import org.apache.flink.table.planner.functions.sql.SqlCumulateTableFunction;
import org.apache.flink.table.planner.functions.sql.SqlHopTableFunction;
import org.apache.flink.table.planner.functions.sql.SqlTumbleTableFunction;
import org.apache.flink.table.planner.functions.sql.SqlWindowTableFunction;

import com.google.common.collect.Iterables;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.tvf.CumulativeWindowSpec;
import org.apache.calcite.rex.tvf.HoppingWindowSpec;
import org.apache.calcite.rex.tvf.RexTvf;
import org.apache.calcite.rex.tvf.TumblingWindowSpec;
import org.apache.calcite.rex.tvf.WindowSpec;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** FlinkRelMdTVFReferences. */
public class FlinkRelMdTVFReferences implements MetadataHandler<BuiltInMetadata.TVFReferences> {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.TVF_REFERENCES.method, new FlinkRelMdTVFReferences());

    // ~ Constructors -----------------------------------------------------------

    protected FlinkRelMdTVFReferences() {}

    // ~ Methods ----------------------------------------------------------------

    public MetadataDef<BuiltInMetadata.TVFReferences> getDef() {
        return BuiltInMetadata.TVFReferences.DEF;
    }

    // Catch-all rule when none of the others apply.
    public Set<RexTvf> getTVFReferences(RelNode rel, RelMetadataQuery mq) {
        return null;
    }

    public Set<RexTvf> getTVFReferences(HepRelVertex rel, RelMetadataQuery mq) {
        return mq.getTVFReferences(rel.getCurrentRel());
    }

    public Set<RexTvf> getTVFReferences(RelSubset rel, RelMetadataQuery mq) {
        return mq.getTVFReferences(Util.first(rel.getBest(), rel.getOriginal()));
    }

    /** TableScan table reference. */
    public Set<RexTvf> getTVFReferences(TableScan rel, RelMetadataQuery mq) {
        return null;
    }

    /** TableFunctionScan table reference. */
    public Set<RexTvf> getTVFReferences(TableFunctionScan rel, RelMetadataQuery mq) {
        assert rel.getInputs().size() == 1;

        RexCall call = (RexCall) rel.getCall();
        WindowSpec windowSpec = convert(call);
        Set<RexTableInputRef.RelTableRef> tableRef = mq.getTableReferences(rel);
        RexCall descriptor = (RexCall) call.getOperands().get(1);

        Set<RexNode> timeAttrRexNode =
                mq.getExpressionLineage(rel.getInput(0), descriptor.getOperands().get(0));
        int index = ((RexTableInputRef) Iterables.getOnlyElement(timeAttrRexNode)).getIndex();
        RexTvf rexTVF = new RexTvf(Iterables.getOnlyElement(tableRef), index, windowSpec);

        HashSet<RexTvf> tvfSet = new HashSet<>();
        tvfSet.add(rexTVF);
        return tvfSet;
    }

    /** Table references from Aggregate. */
    public Set<RexTvf> getTVFReferences(Aggregate rel, RelMetadataQuery mq) {
        return mq.getTVFReferences(rel.getInput());
    }

    /** Table references from Join. */
    public Set<RexTvf> getTVFReferences(Join rel, RelMetadataQuery mq) {
        final RelNode leftInput = rel.getLeft();
        final RelNode rightInput = rel.getRight();
        final Set<RexTvf> result = new HashSet<>();

        // Gather table references, left input references remain unchanged
        final Set<RexTvf> leftTVFRefs = mq.getTVFReferences(leftInput);
        if (leftTVFRefs == null) {
            // We could not infer the table refs from left input
            return null;
        }
        for (RexTvf leftRef : leftTVFRefs) {
            assert !result.contains(leftRef);
            result.add(leftRef);
        }

        // Gather table references, right input references might need to be
        // updated if there are table names clashes with left input
        final Set<RexTvf> rightTVFRefs = mq.getTVFReferences(rightInput);
        if (rightTVFRefs == null) {
            // We could not infer the table refs from right input
            return null;
        }
        for (RexTvf rightRef : rightTVFRefs) {
            assert !result.contains(rightRef);
            result.add(rightRef);
        }

        // Return result
        return result;
    }

    /** Table references from Project. */
    public Set<RexTvf> getTVFReferences(Project rel, final RelMetadataQuery mq) {
        return mq.getTVFReferences(rel.getInput());
    }

    /** Table references from Filter. */
    public Set<RexTvf> getTVFReferences(Filter rel, RelMetadataQuery mq) {
        return mq.getTVFReferences(rel.getInput());
    }

    /** Table references from Sort. */
    public Set<RexTvf> getTVFReferences(Sort rel, RelMetadataQuery mq) {
        return mq.getTVFReferences(rel.getInput());
    }

    private WindowSpec convert(RexCall windowCall) {
        SqlOperator operator = windowCall.getOperator();
        if (!(operator instanceof SqlWindowTableFunction)) {
            throw new IllegalArgumentException(
                    "RexCall "
                            + operator
                            + " is not a window table-valued "
                            + "function, can't convert it into WindowingStrategy");
        }
        if (operator instanceof SqlCumulateTableFunction) {
            long offset = 0;
            if (windowCall.getOperands().size() == 5) {
                offset = getOperandAsLong(windowCall.getOperands().get(4));
            }
            long step = getOperandAsLong(windowCall.getOperands().get(2));
            long maxSize = getOperandAsLong(windowCall.getOperands().get(3));
            return new CumulativeWindowSpec(
                    Duration.ofMillis(maxSize), Duration.ofMillis(step), Duration.ofMillis(offset));
        }

        if (operator instanceof SqlHopTableFunction) {
            long offset = 0;
            if (windowCall.getOperands().size() == 5) {
                offset = getOperandAsLong(windowCall.getOperands().get(4));
            }
            long slide = getOperandAsLong(windowCall.getOperands().get(2));
            long size = getOperandAsLong(windowCall.getOperands().get(3));
            return new HoppingWindowSpec(
                    Duration.ofMillis(size), Duration.ofMillis(slide), Duration.ofMillis(offset));
        }

        if (operator instanceof SqlTumbleTableFunction) {
            long offset = 0;
            if (windowCall.getOperands().size() == 4) {
                offset = getOperandAsLong(windowCall.getOperands().get(3));
            }
            long interval = getOperandAsLong(windowCall.getOperands().get(2));
            return new TumblingWindowSpec(Duration.ofMillis(interval), Duration.ofMillis(offset));
        }
        throw new UnsupportedOperationException(
                "unSupport window: " + operator.getClass().getName());
    }

    private long getOperandAsLong(RexNode node) {
        if (node instanceof RexLiteral) {
            RexLiteral offsetLiteral = (RexLiteral) node;
            if (offsetLiteral.getTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
                return ((BigDecimal) offsetLiteral.getValue()).longValue();
            } else {
                throw new IllegalArgumentException(
                        "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. "
                                + "MONTH and YEAR time unit are not supported yet.");
            }
        } else {
            throw new IllegalArgumentException("Only constant window descriptors are supported.");
        }
    }
}
