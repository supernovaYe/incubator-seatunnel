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
