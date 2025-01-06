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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class ShardRouterTest {

    @Test
    public void test() {

        XXHash64 HASH_INSTANCE = XXHashFactory.fastestInstance().hash64();

        // Assuming there are 1-100 clickhouse nodes
        for (int shardWeightCount = 1; shardWeightCount <= 100; shardWeightCount++) {

            int expectedMinOffset = 0;
            int expectedMaxOffset = shardWeightCount - 1;

            int maxOffset = Integer.MIN_VALUE;
            int minOffset = Integer.MAX_VALUE;

            for (int j = 0; j < 1000000; j++) {

                byte[] randomBytes = new byte[16];
                ThreadLocalRandom.current().nextBytes(randomBytes);

                int offset =
                        (int)
                                ((HASH_INSTANCE.hash(
                                                        ByteBuffer.wrap(
                                                                Arrays.toString(randomBytes)
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8)),
                                                        0)
                                                & Long.MAX_VALUE)
                                        % shardWeightCount);

                if (offset > maxOffset) {
                    maxOffset = offset;
                }
                if (offset < minOffset) {
                    minOffset = offset;
                }
            }

            Assertions.assertEquals(maxOffset, expectedMaxOffset);
            Assertions.assertEquals(minOffset, expectedMinOffset);
        }
    }
}
