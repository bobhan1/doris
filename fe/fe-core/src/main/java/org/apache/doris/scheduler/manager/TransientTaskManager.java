// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.scheduler.manager;

import org.apache.doris.scheduler.disruptor.TaskDisruptor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class TransientTaskManager {
    private static final Logger LOG = LogManager.getLogger(TransientTaskManager.class);
    /**
     * key: taskId
     * value: memory task executor of this task
     * it's used to star task
     */
    private final ConcurrentHashMap<Long, TransientTaskExecutor> taskExecutorMap = new ConcurrentHashMap<>(128);

    /**
     * Producer and Consumer model
     * disruptor is used to handle task
     * disruptor will start a thread pool to handle task
     */
    private TaskDisruptor disruptor;

    public TransientTaskManager() {
        disruptor = new TaskDisruptor();
    }

    public void start() {
        disruptor.start();
    }

    public TransientTaskExecutor getMemoryTaskExecutor(Long taskId) {
        return taskExecutorMap.get(taskId);
    }

    public Long addMemoryTask(TransientTaskExecutor executor) throws JobException {
        Long taskId = executor.getId();
        taskExecutorMap.put(taskId, executor);
        disruptor.tryPublishTask(taskId);
        LOG.info("add memory task, taskId: {}", taskId);
        return taskId;
    }

    public void cancelMemoryTask(Long taskId) throws JobException {
        TransientTaskExecutor transientTaskExecutor = taskExecutorMap.remove(taskId);
        if (transientTaskExecutor != null) {
            transientTaskExecutor.cancel();
        }
    }

    public void removeMemoryTask(Long taskId) {
        taskExecutorMap.remove(taskId);
        LOG.info("remove memory task, taskId: {}", taskId);
    }
}
