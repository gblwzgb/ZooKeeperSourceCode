/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.zookeeper.CreateMode;

/**
 * 解释ZNode的ephemeralOwner字段的抽象。最初，ephemeralOwner注意到ZNode是临时的，并且哪个会话创建了该节点。
 * 通过可选的系统属性（zookeeper.extendedTypesEnabled），可以启用“扩展”功能，例如TTL节点。
 * ephemeralOwner的特殊bit用于表示启用了哪个功能，而ephemeralOwner的其余bit特定于功能。
 *
 * 当系统属性zookeeper.extendedTypesEnabled为true时，将启用扩展类型。
 * 扩展的ephemeralOwner定义为设置了高8位（0xff00000000000000L）的ephemeralOwner。
 * 高8位后面的两个字节用于表示ephemeralOwner代表哪个扩展功能。该功能将剩余的5个字节用于任何目的
 *
 * 当前，唯一的扩展功能是TTL节点。它由扩展特征值0表示。
 * 即，对于TTL节点，ephemeralOwner将高字节设置为0xff，接下来的2个字节为0，后跟5个字节（以毫秒为单位）。
 * 因此，TTL值为1毫秒的ephemeralOwner为：0xff00000000000001。
 *
 * 要添加新的扩展功能，请执行以下操作：
 *      a）向枚举添加新名称，
 *      b）定义一个常量EXTENDED_BIT_xxxx，该常量紧接在后（在TTL之后，将为0x0001），
 *      c）通过静态初始化程序将映射添加到extendedFeatureMap
 *
 * 注意：“容器”节点从技术上讲是扩展类型，但是由于在此功能之前已实现，因此专门对其进行了表示。
 * 根据定义，仅具有高位（0x8000000000000000L）的临时所有者是容器节点（与是否启用扩展类型无关）。
 */

/**
 * <p>
 * Abstraction that interprets the <code>ephemeralOwner</code> field of a ZNode. Originally,
 * the ephemeralOwner noted that a ZNode is ephemeral and which session created the node.
 * Through an optional system property (<code>zookeeper.extendedTypesEnabled</code>) "extended"
 * features such as TTL Nodes can be enabled. Special bits of the ephemeralOwner are used to
 * denote which feature is enabled and the remaining bits of the ephemeralOwner are feature
 * specific.
 * </p>
 * <p>
 * <p>
 * When the system property <code>zookeeper.extendedTypesEnabled</code> is true, extended types
 * are enabled. An extended ephemeralOwner is defined as an ephemeralOwner whose high 8 bits are
 * set (<code>0xff00000000000000L</code>). The two bytes that follow the high 8 bits are
 * used to denote which extended feature the ephemeralOwner represents. The remaining 5 bytes are
 * used by the feature for whatever purpose is needed
 * </p>
 * <p>
 * <p>
 * Currently, the only extended feature is TTL Nodes. It is denoted by the extended feature value of 0.
 * i.e. for TTL Nodes, the ephemeralOwner has the high byte set to 0xff and the next 2 bytes are 0 followed
 * by 5 bytes that represent the TTL value in milliseconds. So, an ephemeralOwner with a TTL value of 1
 * millisecond is: <code>0xff00000000000001</code>.
 * </p>
 * <p>
 * <p>
 * To add new extended features: a) Add a new name to the enum, b) define a constant EXTENDED_BIT_xxxx that's next
 * in line (after TTLs, that would be <code>0x0001</code>), c) add a mapping to the extendedFeatureMap via the static
 * initializer
 * </p>
 * <p>
 * <p>
 * NOTE: "Container" nodes technically are extended types but as it was implemented before this feature they are
 * denoted specially. An ephemeral owner with only the high bit set (<code>0x8000000000000000L</code>) is by definition
 * a container node (irrespective of whether or not extended types are enabled).
 * </p>
 */
public enum EphemeralType {
    /**
     * Not ephemeral
     */
    VOID,
    /**
     * Standard, pre-3.5.x EPHEMERAL
     */
    NORMAL,
    /**
     * Container node
     */
    CONTAINER,
    /**
     * TTL node
     */
    TTL() {
        @Override
        public long maxValue() {
            return EXTENDED_FEATURE_VALUE_MASK;  // 12725 days, about 34 years
        }

        @Override
        public long toEphemeralOwner(long ttl) {
            if ((ttl > TTL.maxValue()) || (ttl <= 0)) {
                throw new IllegalArgumentException("ttl must be positive and cannot be larger than: " + TTL.maxValue());
            }
            //noinspection PointlessBitwiseExpression
            return EXTENDED_MASK
                   | EXTENDED_BIT_TTL
                   | ttl;  // TTL_RESERVED_BIT is actually zero - but it serves to document that the proper extended bit needs to be set
        }

        @Override
        public long getValue(long ephemeralOwner) {
            return getExtendedFeatureValue(ephemeralOwner);
        }
    };

    /**
     * For types that support it, the maximum extended value
     *
     * @return 0 or max
     */
    public long maxValue() {
        return 0;
    }

    /**
     * For types that support it, convert a value to an extended ephemeral owner
     *
     * @return 0 or extended ephemeral owner
     */
    public long toEphemeralOwner(long value) {
        return 0;
    }

    /**
     * For types that support it, return the extended value from an extended ephemeral owner
     *
     * @return 0 or extended value
     */
    public long getValue(long ephemeralOwner) {
        return 0;
    }

    public static final long CONTAINER_EPHEMERAL_OWNER = Long.MIN_VALUE;
    public static final long MAX_EXTENDED_SERVER_ID = 0xfe;  // 254

    private static final long EXTENDED_MASK = 0xff00000000000000L;
    private static final long EXTENDED_BIT_TTL = 0x0000;
    private static final long RESERVED_BITS_MASK = 0x00ffff0000000000L;
    private static final long RESERVED_BITS_SHIFT = 40;

    private static final Map<Long, EphemeralType> extendedFeatureMap;

    static {
        Map<Long, EphemeralType> map = new HashMap<>();
        map.put(EXTENDED_BIT_TTL, TTL);
        extendedFeatureMap = Collections.unmodifiableMap(map);
    }

    private static final long EXTENDED_FEATURE_VALUE_MASK = ~(EXTENDED_MASK | RESERVED_BITS_MASK);

    // Visible for testing
    static final String EXTENDED_TYPES_ENABLED_PROPERTY = "zookeeper.extendedTypesEnabled";
    static final String TTL_3_5_3_EMULATION_PROPERTY = "zookeeper.emulate353TTLNodes";

    /**
     * Return true if extended ephemeral types are enabled
     *
     * @return true/false
     */
    public static boolean extendedEphemeralTypesEnabled() {
        return Boolean.getBoolean(EXTENDED_TYPES_ENABLED_PROPERTY);
    }

    /**
     * Convert a ZNode ephemeral owner to an ephemeral type. If extended types are not
     * enabled, VOID or NORMAL is always returned
     *
     * @param ephemeralOwner the ZNode's ephemeral owner
     * @return type
     */
    public static EphemeralType get(long ephemeralOwner) {
        if (extendedEphemeralTypesEnabled()) {
            if (Boolean.getBoolean(TTL_3_5_3_EMULATION_PROPERTY)) {
                if (EphemeralTypeEmulate353.get(ephemeralOwner) == EphemeralTypeEmulate353.TTL) {
                    return TTL;
                }
            }

            if ((ephemeralOwner & EXTENDED_MASK) == EXTENDED_MASK) {
                long extendedFeatureBit = getExtendedFeatureBit(ephemeralOwner);
                EphemeralType ephemeralType = extendedFeatureMap.get(extendedFeatureBit);
                if (ephemeralType == null) {
                    throw new IllegalArgumentException(String.format("Invalid ephemeralOwner. [%s]", Long.toHexString(ephemeralOwner)));
                }
                return ephemeralType;
            }
        }
        if (ephemeralOwner == CONTAINER_EPHEMERAL_OWNER) {
            return CONTAINER;
        }
        return (ephemeralOwner == 0) ? VOID : NORMAL;
    }

    /**
     * Make sure the given server ID is compatible with the current extended ephemeral setting
     *
     * @param serverId Server ID
     * @throws RuntimeException extendedTypesEnabled is true but Server ID is too large
     */
    public static void validateServerId(long serverId) {
        // TODO: in the future, serverId should be validated for all cases, not just the extendedEphemeralTypesEnabled case
        // TODO: however, for now, it would be too disruptive

        if (extendedEphemeralTypesEnabled()) {
            if (serverId > EphemeralType.MAX_EXTENDED_SERVER_ID) {
                throw new RuntimeException(
                    "extendedTypesEnabled is true but Server ID is too large. Cannot be larger than "
                    + EphemeralType.MAX_EXTENDED_SERVER_ID);
            }
        }
    }

    /**
     * Utility to validate a create mode and a ttl
     *
     * @param mode create mode
     * @param ttl  ttl
     * @throws IllegalArgumentException if the ttl is not valid for the mode
     */
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification = "toEphemeralOwner may throw IllegalArgumentException")
    public static void validateTTL(CreateMode mode, long ttl) {
        if (mode.isTTL()) {
            TTL.toEphemeralOwner(ttl);
        } else if (ttl >= 0) {
            throw new IllegalArgumentException("ttl not valid for mode: " + mode);
        }
    }

    private static long getExtendedFeatureBit(long ephemeralOwner) {
        return (ephemeralOwner & RESERVED_BITS_MASK) >> RESERVED_BITS_SHIFT;
    }

    private static long getExtendedFeatureValue(long ephemeralOwner) {
        return ephemeralOwner & EXTENDED_FEATURE_VALUE_MASK;
    }
}
