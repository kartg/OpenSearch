/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndexClosedException;

import java.util.function.Supplier;

public class RerouteDecider {

    private final Logger logger;
    private final ReplicationTask task;
    private final ReplicationRequest request;
    private final String actionName;
    private final ClusterStateObserver clusterStateObserver;
    private final Supplier<ClusterBlockLevel> globalBlockLevelSupplier;
    private final Supplier<ClusterBlockLevel> indexBlockLevelSupplier;

    public RerouteDecider(
        ReplicationTask task,
        ReplicationRequest request,
        String actionName,
        ClusterStateObserver clusterStateObserver,
        Supplier<ClusterBlockLevel> globalBlockLevelSupplier,
        Supplier<ClusterBlockLevel> indexBlockLevelSupplier,
        Logger logger
    ) {
        this.task = task;
        this.request = request;
        this.actionName = actionName;
        this.clusterStateObserver = clusterStateObserver;
        this.globalBlockLevelSupplier = globalBlockLevelSupplier;
        this.indexBlockLevelSupplier = indexBlockLevelSupplier;
        this.logger = logger;
    }

    private Result retryBecauseUnavailable(ShardId shardId, String message) {
        return Result.retry(
            new UnavailableShardsException(shardId, "{} Timeout: [{}], request: [{}]", message, request.timeout(), request)
        );
    }

    public Result execute() {
        ReplicationTask.setPhaseSafe(task, "routing");
        final ClusterState clusterState = clusterStateObserver.setAndGetObservedState();
        final ClusterBlockException blockException = clusterState.checkForBlockException(
            request.shardId().getIndexName(),
            globalBlockLevelSupplier.get(),
            indexBlockLevelSupplier.get()
        );
        if (blockException != null) {
            if (blockException.retryable()) {
                logger.trace("cluster is blocked, scheduling a retry", blockException);
                return Result.retry(blockException);
            } else {
                return Result.failed(blockException);
            }
        } else {
            final IndexMetadata indexMetadata = clusterState.metadata().index(request.shardId().getIndex());
            if (indexMetadata == null) {
                // ensure that the cluster state on the node is at least as high as the node that decided that the index was there
                if (clusterState.version() < request.routedBasedOnClusterVersion()) {
                    logger.trace(
                        "failed to find index [{}] for request [{}] despite sender thinking it would be here. "
                            + "Local cluster state version [{}]] is older than on sending node (version [{}]), scheduling a retry...",
                        request.shardId().getIndex(),
                        request,
                        clusterState.version(),
                        request.routedBasedOnClusterVersion()
                    );
                    return Result.retry(
                        new IndexNotFoundException(
                            "failed to find index as current cluster state with version ["
                                + clusterState.version()
                                + "] is stale (expected at least ["
                                + request.routedBasedOnClusterVersion()
                                + "]",
                            request.shardId().getIndexName()
                        )
                    );
                } else {
                    return Result.failed(new IndexNotFoundException(request.shardId().getIndex()));
                }
            }

            if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                return Result.failed(new IndexClosedException(indexMetadata.getIndex()));
            }

            if (request.waitForActiveShards() == ActiveShardCount.DEFAULT) {
                // if the wait for active shard count has not been set in the request,
                // resolve it from the index settings
                request.waitForActiveShards(indexMetadata.getWaitForActiveShards());
            }
            assert request.waitForActiveShards() != ActiveShardCount.DEFAULT : "request waitForActiveShards must be set in resolveRequest";

            final ShardRouting primary = clusterState.getRoutingTable().shardRoutingTable(request.shardId()).primaryShard();
            if (primary == null || primary.active() == false) {
                logger.trace(
                    "primary shard [{}] is not yet active, scheduling a retry: action [{}], request [{}], " + "cluster state version [{}]",
                    request.shardId(),
                    actionName,
                    request,
                    clusterState.version()
                );
                return retryBecauseUnavailable(request.shardId(), "primary shard is not active");
            }
            if (clusterState.nodes().nodeExists(primary.currentNodeId()) == false) {
                logger.trace(
                    "primary shard [{}] is assigned to an unknown node [{}], scheduling a retry: action [{}], request [{}], "
                        + "cluster state version [{}]",
                    request.shardId(),
                    primary.currentNodeId(),
                    actionName,
                    request,
                    clusterState.version()
                );
                return retryBecauseUnavailable(request.shardId(), "primary shard isn't assigned to a known node.");
            }
            if (primary.currentNodeId().equals(clusterState.nodes().getLocalNodeId())) {
                return Result.local(clusterState);
            } else {
                return Result.remote(clusterState);
            }
        }
    }

    public static class Result {
        private final Decision decision;
        private final ClusterState clusterStateSnapshot;
        private final Exception cause;

        public Decision decision() {
            return decision;
        }

        public ClusterState clusterState() {
            return clusterStateSnapshot;
        }

        public Exception cause() {
            return cause;
        }

        public Result(Decision decision, ClusterState clusterStateSnapshot, Exception cause) {
            this.decision = decision;
            this.clusterStateSnapshot = clusterStateSnapshot;
            this.cause = cause;
        }

        public static Result local(ClusterState clusterState) {
            return new Result(Decision.LOCAL, clusterState, null);
        }

        public static Result remote(ClusterState clusterState) {
            return new Result(Decision.REMOTE, clusterState, null);
        }

        public static Result retry(Exception e) {
            return new Result(Decision.RETRY, null, e);
        }

        public static Result failed(Exception e) {
            return new Result(Decision.FAILED, null, e);
        }
    }

    public enum Decision {
        LOCAL,
        REMOTE,
        RETRY,
        FAILED
    }
}
