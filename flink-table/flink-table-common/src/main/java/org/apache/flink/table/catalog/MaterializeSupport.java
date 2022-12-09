package org.apache.flink.table.catalog;

import java.util.Map;

/** support materialize interface. */
public interface MaterializeSupport {

    void createMaterializeView(
            CatalogMaterializedView mv, ObjectIdentifier id, boolean ignoreIfExists);

    Map<ObjectIdentifier, String> listAllMaterializeViews();
}
