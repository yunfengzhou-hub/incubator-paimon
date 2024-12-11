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

package org.apache.paimon.flink.lookup.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link ReplicatedBucketShuffleStrategy} defines a strategy for determining the target subtask id
 * for a given bucket id and join key hash. It also provides a method to retrieve the set of bucket
 * ids that are required to be cached by a specific subtask.
 *
 * <p>The strategy will randomly select a subtask base on the number of replicate.
 */
public class ReplicatedBucketShuffleStrategy implements ShuffleStrategy {

    private static final Logger LOG =
            LoggerFactory.getLogger(ReplicatedBucketShuffleStrategy.class);

    private final int numBuckets;
    private final Random random;
    private final int numReplicate;

    public ReplicatedBucketShuffleStrategy(int numBuckets, int numReplicate) {
        checkState(numBuckets > 0, "Number of buckets should be positive.");
        checkState(numReplicate > 0, "Number of replicate should be positive.");
        this.numBuckets = numBuckets;
        this.numReplicate = numReplicate;
        this.random = new Random();
    }

    @Override
    public int getTargetSubtaskId(int bucketId, int joinKeyHash, int numSubtasks) {
        checkState(numSubtasks > 0, "Number of subtasks should be positive.");

        int effectiveReplicateNum = getEffectiveReplicateNum(numSubtasks);
        int subtaskIdxBase = getSubtaskIdxBase(bucketId, numSubtasks, effectiveReplicateNum);

        return (subtaskIdxBase + random.nextInt(effectiveReplicateNum)) % numSubtasks;
    }

    @Override
    public Set<Integer> getRequiredCacheBucketIds(int subtaskId, int numSubtasks) {
        checkState(numSubtasks > 0, "Number of subtasks should be positive.");

        Set<Integer> requiredCacheBucketIds = new HashSet<>();
        int effectiveReplicateNum = getEffectiveReplicateNum(numSubtasks);
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            if (bucketBelongsToSubtask(bucketId, subtaskId, numSubtasks, effectiveReplicateNum)) {
                requiredCacheBucketIds.add(bucketId);
            }
        }

        LOG.info("Required cache bucket ids for subtask {}: {}", subtaskId, requiredCacheBucketIds);
        return requiredCacheBucketIds;
    }

    int getEffectiveReplicateNum(int numSubtasks) {
        return Math.max(
                (int) Math.ceil(numSubtasks * 1.0 / numBuckets),
                Math.min(numReplicate, numSubtasks));
    }

    private int getSubtaskIdxBase(int bucketId, int numSubtasks, int effectiveReplicateNum) {
        return bucketId * effectiveReplicateNum % numSubtasks;
    }

    private boolean bucketBelongsToSubtask(
            int bucketId, int subtaskId, int numSubtasks, int effectiveReplicateNum) {
        int subtaskIdxBase = getSubtaskIdxBase(bucketId, numSubtasks, effectiveReplicateNum);
        int endIndex = subtaskIdxBase + effectiveReplicateNum;

        if (endIndex <= numSubtasks) {
            return subtaskId >= subtaskIdxBase && subtaskId < endIndex;
        } else {
            return subtaskId >= subtaskIdxBase || subtaskId < endIndex % numSubtasks;
        }
    }
}
