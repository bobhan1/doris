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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.Int16OrLessType;
import org.apache.doris.nereids.types.coercion.IntegralType;

/**
 * TinyInt type in Nereids.
 */
public class TinyIntType extends IntegralType implements Int16OrLessType {
    public static final TinyIntType INSTANCE = new TinyIntType();

    public static final int RANGE = 3; // The maximum number of digits that TinyIntType can represent.
    private static final int WIDTH = 1;

    private TinyIntType() {
    }

    @Override
    public Type toCatalogDataType() {
        return Type.TINYINT;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TinyIntType;
    }

    @Override
    public String simpleString() {
        return "tinyint";
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof TinyIntType;
    }

    @Override
    public DataType defaultConcreteType() {
        return this;
    }

    @Override
    public int width() {
        return WIDTH;
    }
}
