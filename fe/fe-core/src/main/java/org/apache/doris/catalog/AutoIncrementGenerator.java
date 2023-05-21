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

package org.apache.doris.catalog;

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.AutoIncrementIdUpdateLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class AutoIncrementGenerator implements Writable {

    public static final long NEXT_ID_INIT_VALUE = 1;
    // _MIN_BATCH_SIZE = 4064 in load task
    private static final long BATCH_ID_INTERVAL = 50000;

    @SerializedName(value = "dbId")
    private Long dbId;
    @SerializedName(value = "tableId")
    private Long tableId;
    @SerializedName(value = "nextIdMap")
    private Map<String, IdGeneratorBuffer> nextIdMap; // column name to its current max id

    private EditLog editLog;

    public AutoIncrementGenerator() {
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    public void applyChange(String columnName, long batchNextId) {
        IdGeneratorBuffer buffer = nextIdMap.get(columnName);
        if (buffer.getBatchEndId() < batchNextId) {
            buffer.setNextId(batchNextId);
            buffer.setBatchEndId(batchNextId);
        }
    }

    public synchronized Pair<Long, Long> getAutoIncrementRange(String columnName, long length, long lower_bound) throws UserException {
        IdGeneratorBuffer buffer = nextIdMap.get(columnName);
        long nextId = Math.Max(buffer.getNextId(length), lower_bound);
        long endId = nextId + length;
        buffer.setNextId(endId);
        long batchEndId = buffer.getBatchEndId()
        if (endId > batchEndId) {
            batchEndId = (endId / BATCH_ID_INTERVAL + 1) * BATCH_ID_INTERVAL;
            buffer.setBatchEndId(batchEndId);
            if (editLog != null) {
                AutoIncrementIdUpdateLog info = new AutoIncrementIdUpdateLog(dbId, tableId, columnName, batchEndId);
                editLog.logUpdateAutoIncrementId(info);
            }
        }
        return Pair.of(nextId, length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AutoIncrementGenerator read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AutoIncrementGenerator.class);
    }

    public class IdGeneratorBuffer {
        private long nextId;
        private long batchEndId;

        private IdGeneratorBuffer(long nextId, long batchEndId) {
            this.nextId = nextId;
            this.batchEndId = batchEndId;
        }

        public long getNextId(long length) {
            return nextId;
        }

        public long getBatchEndId() {
            return batchEndId;
        }

        public void setNextId(long nextId) {
            this.nextId = nextId;
        }

        public void setBatchEndId(long batchEndId) {
            this.batchEndId = batchEndId;
        }
    }
}
