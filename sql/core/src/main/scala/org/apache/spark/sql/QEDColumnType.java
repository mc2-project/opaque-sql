/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql;

public enum QEDColumnType {
    DUMMY(0),
    INT(1),
    STRING(2),
    FLOAT(3),
    DATE(4),
    PARTIAL_AGG_INT(5),
    FINAL_AGG_INT(6),
    PARTIAL_AGG_FLOAT(7),
    FINAL_AGG_FLOAT(8);

    private int value;

    private QEDColumnType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
