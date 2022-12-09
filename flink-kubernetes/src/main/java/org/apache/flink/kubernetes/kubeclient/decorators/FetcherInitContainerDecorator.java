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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Attach the command and args to the main container for running the JobManager. */
public class FetcherInitContainerDecorator extends AbstractKubernetesStepDecorator {

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters;

    public FetcherInitContainerDecorator(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        Optional<Container> fetcherInitContainer = flinkPod.getFetcherInitContainer();
        if (kubernetesJobManagerParameters
                        .getDeploymentTarget()
                        .equals(KubernetesDeploymentTarget.APPLICATION)
                && fetcherInitContainer.isPresent()) {
            final Container fetcherInitContainerWithStartCmd =
                    new ContainerBuilder(flinkPod.getFetcherInitContainer().get())
                            .withCommand(kubernetesJobManagerParameters.getContainerEntrypoint())
                            .withArgs(getFetcherInitContainerStartCommand())
                            .build();
            new FlinkPod.Builder(flinkPod)
                    .withFetcherInitContainer(flinkPod, fetcherInitContainerWithStartCmd)
                    .build();
        }
        return flinkPod;
    }

    private List<String> getFetcherInitContainerStartCommand() {
        Configuration flinkConf = kubernetesJobManagerParameters.getFlinkConfiguration();
        Map<String, String> config = new HashMap<>();
        config.put(
                SecurityOptions.KERBEROS_LOGIN_KEYTAB.key(),
                flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB));
        config.put(
                SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key(),
                flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL));
        config.put(
                PipelineOptions.JARS.key(), assembleJarsStr(flinkConf.get(PipelineOptions.JARS)));

        String params = ConfigurationUtils.assembleDynamicConfigsStr(config);
        return KubernetesUtils.getStartCommandWithBashWrapper(
                Constants.KUBERNETES_INIT_CONTAINER_FETCHER_SCRIPT_PATH + " " + params);
    }

    private static String assembleJarsStr(final List<String> config) {
        return config.stream().map(e -> String.format("%s", e)).collect(Collectors.joining(";"));
    }
}
