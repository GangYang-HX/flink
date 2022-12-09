package com.bilibili.bsql.clickhouse.shard.connection;

import com.bilibili.bsql.clickhouse.shard.tableinfo.ClickHouseShardTableInfo;
import com.bilibili.bsql.common.utils.JDBCLock;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClickHouseConnectionProvider implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);
	private static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
	private static final Pattern PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");
	private static final String GET_SHARD_HOST_SQL = "SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = ? ORDER BY shard_num, host_address";
	private static final String GET_SHARD_CLUSTER_SQL = "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?";
	private static final Pattern DISTRIBUTED_ENGINE_PATTERN = Pattern.compile("Distributed\\((.*?), (.*?), (.*?)(, (.*))?\\)");
	private static final Pattern SHARDING_FUNC_PATTERN = Pattern.compile(", (.*)");
	private static final Pattern SHARDING_KEY_PATTERN = Pattern.compile("\\w*\\(*(.+)*\\)");
	public static String shardingFunc;

    // runtime fields
    private final transient static Map<String, ClickHouseConnection> connections;
    private final transient static Map<String, Multimap<Integer, ClickHouseConnection>> shardConnections;

	static {
        connections = new ConcurrentHashMap<>(4);
        shardConnections = new ConcurrentHashMap<>(4);
    }

	public static ClickHouseConnection getConnection(ClickHouseShardTableInfo tableInfo) throws SQLException {
	    String cacheKey = generateKey(tableInfo.getUrl(), tableInfo.getDatabase(), tableInfo.getTableName());
        ClickHouseConnection connection = connections.get(cacheKey);
        if (connection == null) {
            synchronized (ClickHouseConnectionProvider.class) {
                //double check
                if (connections.get(cacheKey) == null) {
                    connections.put(cacheKey, createConnection(tableInfo));
                    LOG.info("create a new db connection: {}/{}", tableInfo.getUrl(), tableInfo.getDatabase());
                }
                return connections.get(cacheKey);
            }
        }
        return connection;
	}

    private static ClickHouseConnection createConnection(ClickHouseShardTableInfo tableInfo) throws SQLException {
        return createConnection(tableInfo, tableInfo.getUrl(), tableInfo.getDatabase());
    }

    private static ClickHouseConnection createConnection(ClickHouseShardTableInfo tableInfo, String url, String database) throws SQLException {
        try {
            synchronized (JDBCLock.class) {
                Class.forName(CLICKHOUSE_DRIVER_NAME);
            }
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        ClickHouseConnection conn;
        if (StringUtils.isNoneBlank(tableInfo.getUsername())) {
            //已校验
            conn = (ClickHouseConnection) DriverManager.getConnection(getJdbcUrl(tableInfo, url, database),
                    tableInfo.getUsername(), tableInfo.getPassword());
        } else {
            conn = (ClickHouseConnection) DriverManager.getConnection(getJdbcUrl(tableInfo, url, database));
        }
        return conn;
    }

    public static Multimap<Integer, ClickHouseConnection> getShardConnections(ClickHouseShardTableInfo tableInfo,
                                                                              String remoteCluster,
                                                                              String remoteDatabase) throws SQLException {
        String cacheKey = generateKey(remoteCluster, remoteDatabase);
        Multimap<Integer, ClickHouseConnection> shardConnection = shardConnections.get(cacheKey);
        if (shardConnection == null) {
            synchronized (ClickHouseConnectionProvider.class) {
                if (shardConnections.get(cacheKey) == null) {
                    Multimap<Integer, ClickHouseConnection> cacheConn = LinkedListMultimap.create();
                    final ClickHouseConnection conn = getConnection(tableInfo);
                    final PreparedStatement stmt = conn.prepareStatement(GET_SHARD_HOST_SQL);
                    stmt.setString(1, remoteCluster);
                    try (final ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            final int shardNum = rs.getInt("shard_num");
                            final String host = rs.getString("host_address");
                            final int port = getActualHttpPort(host, rs.getInt("port"));
                            final String url = "clickhouse://" + host + ":" + port;
                            cacheConn.put(shardNum, createConnection(tableInfo, url, remoteDatabase));
                            LOG.info("create a new replica connection of shard {}: {}/{}", shardNum, url, remoteCluster);
                        }
                    }
                    if (cacheConn.isEmpty()) {
                        throw new SQLException("unable to query shards in system.clusters");
                    }
                    shardConnections.put(cacheKey, cacheConn);
                }
            }
        }
        return shardConnections.get(cacheKey);
    }

	private static int getActualHttpPort(final String host, final int port) throws SQLException {
		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {
			final HttpGet request = new HttpGet(new URIBuilder().setScheme("http").setHost(host).setPort(port).build());
			final HttpResponse response = httpclient.execute(request);
			final int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == 200) {
				return port;
			}
			final String raw = EntityUtils.toString(response.getEntity());
			final Matcher matcher = ClickHouseConnectionProvider.PATTERN.matcher(raw);
			if (matcher.find()) {
				return Integer.parseInt(matcher.group("port"));
			}
			throw new SQLException("Cannot query ClickHouse http port");
		} catch (Exception e) {
			throw new SQLException("Cannot connect to ClickHouse server using HTTP", e);
		}
	}

	public static void closeConnections() throws SQLException {
		if (!connections.isEmpty()) {
			connections.forEach((key, connection) -> {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            });
		}
		if (!shardConnections.isEmpty()) {
			shardConnections.forEach((key, connections) -> {
			    connections.values().forEach(connection -> {
                    try {
                        connection.close();
                    } catch (SQLException ignore) {
                    }
                });
            });
		}
	}

	private static String getJdbcUrl(ClickHouseShardTableInfo tableInfo, String url, String database) throws SQLException {
		try {
			return "jdbc:" + new URIBuilder(url).setPath("/" + database).build().toString() + "?charset=" +
                    tableInfo.getCharset() + "&socketTimeout=" + tableInfo.getSocketTimeout() +
                    "&connectionTimeout=" + tableInfo.getConnectionTimeout();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public static String queryTableEngine(ClickHouseShardTableInfo tableInfo) throws SQLException {
		final ClickHouseConnection conn = getConnection(tableInfo);
		try (final PreparedStatement stmt = conn.prepareStatement(GET_SHARD_CLUSTER_SQL)) {
			stmt.setString(1, tableInfo.getDatabase());
			stmt.setString(2, tableInfo.getTableName());
			try (final ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					return rs.getString("engine_full");
				}
			}
		}
		throw new SQLException("table `" + tableInfo.getDatabase() + "`.`" + tableInfo.getTableName() + "` does not exist");
	}

	public static Optional<List<String>> getShardingKeys(String engineFull) {
		Optional<String> shardingKeyRawOpt = findPatternMatchGroup(engineFull, DISTRIBUTED_ENGINE_PATTERN, 4);
		if (shardingKeyRawOpt.isPresent()) {
			Optional<String> shardingFuncOpt = findPatternMatchGroup(shardingKeyRawOpt.get(), SHARDING_FUNC_PATTERN, 1);
			if (shardingFuncOpt.isPresent()) {
				shardingFunc = shardingFuncOpt.get();
				return findShardingKeys(shardingFuncOpt.get());
			}
		}
		return Optional.empty();
	}

	private static Optional<List<String>> findShardingKeys(String shardingKeysFuncString) {
		Optional<String> shardingFuncOpt = findPatternMatchGroup(shardingKeysFuncString, SHARDING_KEY_PATTERN, 1);
		if (shardingFuncOpt.isPresent() && !shardingFuncOpt.get().contains("(")) {
			List<String> shardingKeys = Arrays.stream(shardingFuncOpt.get().split(","))
				.map(String::trim).collect(Collectors.toList());
			return Optional.of(shardingKeys);

		} else {
			return shardingFuncOpt.isPresent() ? findShardingKeys(shardingFuncOpt.get()) : Optional.empty();
		}
	}

	private static Optional<String> findPatternMatchGroup(String candidate, Pattern pattern, int group) {
		Matcher m = pattern.matcher(candidate);
		if (m.find()) {
			String groupVal = m.group(group);
			if (!StringUtils.isBlank(groupVal)) return Optional.of(groupVal);
		}
		return Optional.empty();
	}

    private static String generateKey(String... keys) {
        StringBuilder encodeKey = new StringBuilder();
        Arrays.stream(keys).forEach(key -> encodeKey.append(Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8))));
        return encodeKey.toString();
    }
}
