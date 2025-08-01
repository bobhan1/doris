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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;

import com.google.common.base.Strings;

import java.util.Map;

// Drop one column
public class DropColumnClause extends AlterTableClause {
    private String colName;
    private String rollupName;

    private Map<String, String> properties;

    public String getColName() {
        return colName;
    }

    public String getRollupName() {
        return rollupName;
    }

    public DropColumnClause(String colName, String rollupName, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.colName = colName;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    @Override
    public void analyze() throws AnalysisException {
        if (Strings.isNullOrEmpty(colName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                    colName, FeNameFormat.getColumnNameRegex());
        }
        if (Strings.isNullOrEmpty(rollupName)) {
            rollupName = null;
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return true;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP COLUMN `").append(colName).append("`");
        if (rollupName != null) {
            sb.append(" FROM `").append(rollupName).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
