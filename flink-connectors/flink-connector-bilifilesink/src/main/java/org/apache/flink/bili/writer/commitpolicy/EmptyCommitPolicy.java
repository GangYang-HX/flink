/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.commitpolicy;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author zhouxiaogang
 * @version $Id: EmptyCommitPolicy.java, v 0.1 2020-06-11 18:47
zhouxiaogang Exp $$
 */
public class EmptyCommitPolicy implements PartitionCommitPolicy {
	private static final Logger LOG = LoggerFactory.getLogger(EmptyCommitPolicy.class);
	private FileSystem fileSystem;

	public EmptyCommitPolicy(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public void commit(PartitionCommitPolicy.Context context) throws Exception {
		LOG.info("{} write finish", context.partitionPath());
		FileStatus[] allFileInPartition;

		try {
			allFileInPartition = fileSystem.listStatus(context.partitionPath());
		} catch (IOException e) {
			LOG.error("{} commit failure",context.partitionPath(), e);
			return;
		}

		for (FileStatus fileStatus : allFileInPartition) {
			if (isProgressFile(fileStatus) && !isBackupFile(fileStatus)) {
				LOG.info("deleting inprogress file {} created at {}", fileStatus.getPath(),
					new Timestamp(fileStatus.getModificationTime()));
				try {
					fileSystem.delete(fileStatus.getPath(),false);
				} catch (IOException e) {
					LOG.error("{} delete failure",fileStatus, e);
				}
			}
		}
	}

	private static boolean isProgressFile(FileStatus fileStatus) {
		String name = fileStatus.getPath().getName();
		return name.contains("inprogress");
	}

	private static boolean isBackupFile(FileStatus fileStatus) {
		String name = fileStatus.getPath().getParent().getName();
		return name.contains("bak");
	}

	private static boolean isHiddenFile(FileStatus fileStatus) {
		String name = fileStatus.getPath().getName();
		return name.startsWith("_") || name.startsWith(".");
	}
}
