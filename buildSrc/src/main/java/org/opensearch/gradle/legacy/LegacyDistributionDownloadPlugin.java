/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.legacy;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.opensearch.gradle.DistributionDownloadPlugin;

public class LegacyDistributionDownloadPlugin implements Plugin<Project> {

    // for downloading Elasticsearch OSS distributions to run BWC
    private static final String FAKE_IVY_GROUP_ES = "elasticsearch-distribution";
    private static final String DOWNLOAD_REPO_NAME_ES = "elasticsearch-downloads";
    private static final String SNAPSHOT_REPO_NAME_ES = "elasticsearch-snapshots";
    private static final String FAKE_SNAPSHOT_IVY_GROUP_ES = "elasticsearch-distribution-snapshot";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(DistributionDownloadPlugin.class);
        addIvyRepo(project, DOWNLOAD_REPO_NAME_ES, "https://artifacts-no-kpi.elastic.co", FAKE_IVY_GROUP_ES);
        addIvyRepo(project, SNAPSHOT_REPO_NAME_ES, "https://snapshots-no-kpi.elastic.co", FAKE_SNAPSHOT_IVY_GROUP_ES);
    }

    private static void addIvyRepo(Project project, String name, String url, String group) {
        IvyArtifactRepository ivyRepo = project.getRepositories().ivy(repo -> {
            repo.setName(name);
            repo.setUrl(url);
            repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
            repo.patternLayout(layout -> layout.artifact("/downloads/elasticsearch/elasticsearch-oss-[revision](-[classifier]).[ext]"));
        });
        project.getRepositories().exclusiveContent(exclusiveContentRepository -> {
            exclusiveContentRepository.filter(config -> config.includeGroup(group));
            exclusiveContentRepository.forRepositories(ivyRepo);
        });
    }
}
