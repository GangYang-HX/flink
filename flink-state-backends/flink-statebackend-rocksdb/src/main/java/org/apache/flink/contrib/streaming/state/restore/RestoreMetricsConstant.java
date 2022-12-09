package org.apache.flink.contrib.streaming.state.restore;

public class RestoreMetricsConstant {

    // ------------------------------- rescale --------------------------------------------
    public static final String RESCALE_GROUP = "rescale";

    public static final String CLIP_DB_TIME = "clipDBTime";
    public static final String LOAD_DB_TIME = "loadDBTime";
    public static final String ITERATE_DB_TIME = "iterateDBTime";


    // ------------------------------- no-rescale --------------------------------------------
    public static final String NO_RESCALE_GROUP = "norescale";

    public static final String LINK_OR_COPY_FILES_TIME = "linkOrCopyFileTime";
    public static final String REGIST_CF_HANDLE_TIME = "registCFHandleTime";

    // ------------------------------- common --------------------------------------------
    public static final String INIT_ROCKS_DB_TIME = "initRocksDBTime";
    public static final String READ_METADATA_TIME = "readMetadataTime";
    public static final String REGIST_CF_DESC_TIME = "registCFDescTime";
}
