package org.apache.flink.bilibili.catalog.client;

import io.grpc.ManagedChannel;
import org.apache.commons.lang3.StringUtils;
import pleiades.component.env.v1.Env;

import java.util.Properties;

/**
 * @author : luotianran
 * @version V1.0
 * @Description:
 * @date Date : 2021年09月15日
 */
public abstract class AbstractDatacenterRpcCli {

	/**
	 * Datacenter参数初始化
	 */
	protected void open() {
		//discovery 环境变量
		Properties properties = new Properties();
		String deployEnv = System.getenv("DEPLOY_ENV");
		if (StringUtils.isBlank(deployEnv)) {
			deployEnv = "prod";
		}
		properties.setProperty("deploy_env", deployEnv);
		properties.setProperty("discovery_zone", "sh001");
		Env.reload(properties);
	}

	//资源回收
	protected abstract void close();


}
