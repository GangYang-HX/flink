package org.apache.flink.bilibili.starter;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.bili.external.excalibur.configuration.ExcaliburConfigOptions;
import org.apache.flink.bilibili.catalog.BilibiliCatalog;
import org.apache.flink.bilibili.sql.SqlRegister;
import org.apache.flink.bilibili.sql.SqlTree;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author zhangyang
 * @Date:2021/5/31
 * @Time:3:36 下午
 */
public class YarnSessionStarter {
    private static final Logger LOG = LoggerFactory.getLogger(YarnSessionStarter.class);
    private final static String TABLE_EXEC_CONF_PREFIX = "table.exec.";
    private final static String TABLE_OPTIMIZE_CONF_PREFIX = "table.optimizer.";
    private final static String LATENCY_PREFIX = "latency.";
	private final static String JOIN_PREFIX = "join.";
	private final static String IDLE_STATE_RETENTION = "idle.state.retention";
    public static final String CONTEXT_FILE = "context.json";
    public static final String FLINK_CONF_FILE = "flink-conf.yaml";
	public static final String CUSTOM_JOB_ID = "customJobId";
	public static final String PIPELINE_GLOBAL_JOB_PARAMETERS = "pipeline.global-job-parameters";

    public static void main(String[] args) throws Exception {

		//参数初始化
		final ParameterTool params = ParameterTool.fromArgs(args);

    	String workPath = params.get("work_path");
    	if (StringUtils.isBlank(workPath)) {
			throw new RuntimeException("workPath is not exist");
		}

    	//job的透传参数
		String extraConfig = params.get("extra_config");

        File contextFile = new File(workPath, CONTEXT_FILE);
        File flinkConfFile = new File(workPath, FLINK_CONF_FILE);
        //test code
//		contextFile = new File(args[0]);
//		flinkConfFile = new File(args[1]);
        if (!contextFile.exists() || !flinkConfFile.exists()) {
            throw new RuntimeException("bsql context exist: " + contextFile.exists()
                    + "; flink-conf exist: " + flinkConfFile.exists());
        }

        String context =
                IOUtils.toString(new FileInputStream(contextFile), StandardCharsets.UTF_8);
        LOG.info("YarnStarter context: {}", context);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(context);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
		tabEnv.registerCatalog("bilibili", new BilibiliCatalog("bilibili"));

        Properties properties = new Properties();
        properties.load(new FileInputStream(flinkConfFile));

        LOG.info("YarnStarter properties: {}", properties);

        handleUnSupportConfigFileSettings(tabEnv, properties, root, extraConfig);

        StatementSet statementSet = tabEnv.createStatementSet();

        SqlRegister register = new SqlRegister(env, tabEnv, statementSet);

        String sql = root.get("sql").asText();
        SqlTree sqlTree = new SqlTree(sql);
        register.register(sqlTree);

        String jobName = root.get("name").asText();
		if (jobName == null || "null".equals(jobName)) {
			statementSet.execute();
		} else {
			statementSet.execute(jobName);
		}
    }

    /**
     * 部分参数只有代码入口可以设置,这个地方做下这个事情,等flink都支持配置传递后就可以去掉了
     */
    private static void handleUnSupportConfigFileSettings(StreamTableEnvironment tabEnv, Properties properties, JsonNode jobContext, String extraConfig) throws JsonProcessingException {
        tabEnv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer", "drop");
        //handle blink optimize config
        for (String key : properties.stringPropertyNames()) {
			if (key.toLowerCase().startsWith(TABLE_EXEC_CONF_PREFIX)
					|| key.toLowerCase().startsWith(PIPELINE_GLOBAL_JOB_PARAMETERS)
					|| key.toLowerCase().startsWith(TABLE_OPTIMIZE_CONF_PREFIX)
					|| key.toLowerCase().startsWith(LATENCY_PREFIX)
					|| key.toLowerCase().startsWith(JOIN_PREFIX)) {
				tabEnv.getConfig().getConfiguration().setString(key, properties.getProperty(key));
			}
        }

		LOG.info("tabEnv configuration: {}", tabEnv.getConfig().getConfiguration().toString());

		//customJobId
		if (Objects.nonNull(jobContext.get(CUSTOM_JOB_ID))) {
			tabEnv.getConfig().addJobParameter(ExecutionOptions.CUSTOM_CALLER_CONTEXT_JOB_ID.key(), jobContext.get(CUSTOM_JOB_ID).asText());
		}

		//extraConfig
		if (StringUtils.isNotBlank(extraConfig)) {
			tabEnv.getConfig().addJobParameter(ExcaliburConfigOptions.JOB_CUSTOM_EXTRA_CONFIG.key(), extraConfig);
		}

        tabEnv.getConfig().setIdleStateRetentionTime(Time.hours(24), Time.hours(26));
        //handle stat idle config
        if (properties.containsKey(IDLE_STATE_RETENTION)) {
            long min = Long.parseLong(properties.getProperty(IDLE_STATE_RETENTION));
            long max = min + 600_000;
            tabEnv.getConfig().setIdleStateRetentionTime(Time.milliseconds(min), Time.milliseconds(max));
        }

		LOG.info("final tabEnv configuration: {}", tabEnv.getConfig().getConfiguration().toString());
	}
}
