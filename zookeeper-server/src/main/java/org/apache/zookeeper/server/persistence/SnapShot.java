/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.zookeeper.server.DataTree;

/**
 * 持久层的快照接口。实现此接口以实现快照。
 */

/**
 * snapshot interface for the persistence layer.
 * implement this interface for implementing
 * snapshots.
 */
public interface SnapShot {

    /**
     * 从最后一个有效快照反序列化到datatree，并返回最后反序列化的zxid
     *
     * @param dt 要反序列化到的datatree
     * @param sessions 要反序列化的会话
     * @return 从快照反序列化的最后一个zxid
     * @throws IOException
     */
    /**
     * deserialize a data tree from the last valid snapshot and
     * return the last zxid that was deserialized
     * @param dt the datatree to be deserialized into
     * @param sessions the sessions to be deserialized into
     * @return the last zxid that was deserialized from the snapshot
     * @throws IOException
     */
    long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException;

    /**
     * 将datatree和会话持久化到持久化存储中
     *
     * @param dt 要序列化的datatree
     * @param sessions 要序列化的会话超时
     * @param name 将快照存储到的对象名称（文件名称？）
     * @param fsync 写入后立即同步快照
     * @throws IOException
     */
    /**
     * persist the datatree and the sessions into a persistence storage
     * @param dt the datatree to be serialized
     * @param sessions the session timeouts to be serialized
     * @param name the object name to store snapshot into
     * @param fsync sync the snapshot immediately after write
     * @throws IOException
     */
    void serialize(DataTree dt, Map<Long, Integer> sessions, File name, boolean fsync) throws IOException;

    /**
     * 查找最新的快照文件
     * @return 最新的快照文件
     * @throws IOException
     */
    /**
     * find the most recent snapshot file
     * @return the most recent snapshot file
     * @throws IOException
     */
    File findMostRecentSnapshot() throws IOException;

    /**
     * 获取上次保存/还原的快照的信息
     * @return 最后快照的信息
     */
    /**
     * get information of the last saved/restored snapshot
     * @return info of last snapshot
     */
    SnapshotInfo getLastSnapshotInfo();

    /**
     * 立即从该快照释放资源
     * @throws IOException
     */
    /**
     * free resources from this snapshot immediately
     * @throws IOException
     */
    void close() throws IOException;

}
