package org.apache.flink.table.catalog;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;
import java.util.Optional;

/** support materialize interface. */
public interface MaterializeSupport {

    void createMaterializeView(
            CatalogMaterializedView mv, ObjectIdentifier id, boolean ignoreIfExists);

    List<Tuple3<ObjectIdentifier, String, Long>> listAllMaterializeViews(Long flagTime);

    Optional<String> getMaterializeViewWatermark(String fullTableName);
}
