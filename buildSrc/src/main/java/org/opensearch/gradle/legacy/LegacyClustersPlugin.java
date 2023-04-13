/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.legacy;

import org.gradle.api.Project;
import org.opensearch.gradle.testclusters.TestClustersPlugin;

public class LegacyClustersPlugin extends TestClustersPlugin {
    @Override
    protected String getExtensionName() {
        return "legacyClusters";
    }

    @Override
    protected void applyDistributionDownloadPlugin(Project project) {
        project.getPlugins().apply(LegacyDistributionDownloadPlugin.class);
    }
}
