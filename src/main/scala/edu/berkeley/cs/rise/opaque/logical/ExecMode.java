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

package edu.berkeley.cs.rise.opaque.logical;

import java.util.Map;
import java.util.HashMap;

public enum ExecMode {
    INSECURE(0),
    ENCRYPTION(1),
    OBLIVIOUS(2),
    FULL_OBLIVIOUS(3);

    private int _value;

    private ExecMode(int _value) {
        this._value = _value;
    }

    public int value() {
        return _value;
    }

    private static Map<Integer, ExecMode> map = new HashMap<Integer, ExecMode>();

    static {
        for (ExecMode m : ExecMode.values()) {
            map.put(m.value(), m);
        }
    }

    public static ExecMode getMode(int value) {
        return map.get(value);
    }

}
