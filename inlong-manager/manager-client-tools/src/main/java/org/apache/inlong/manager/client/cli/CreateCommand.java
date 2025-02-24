/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.client.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.inner.client.InlongClusterClient;
import org.apache.inlong.manager.client.cli.pojo.CreateGroupConf;
import org.apache.inlong.manager.client.cli.util.ClientUtils;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterTagRequest;

import java.io.File;
import java.util.List;

/**
 * Create resource by json file.
 */
@Parameters(commandDescription = "Create resource by json file")
public class CreateCommand extends AbstractCommand {

    @Parameter()
    private List<String> params;

    public CreateCommand() {
        super("create");
        jcommander.addCommand("group", new CreateGroup());
        jcommander.addCommand("cluster", new CreateCluster());
        jcommander.addCommand("cluster-tag", new CreateClusterTag());
        jcommander.addCommand("cluster-node", new CreateClusterNode());
    }

    @Parameters(commandDescription = "Create group by json file")
    private static class CreateGroup extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-f", "--file"},
                converter = FileConverter.class,
                description = "json file")
        private File file;

        @Parameter(names = {"-s"}, description = "optional log string to create file")
        private String input;

        @Override
        void run() {
            try {
                String content;
                if (input != null) {
                    content = input;
                } else {
                    content = ClientUtils.readFile(file);
                }
                // first extract group config from the file passed in
                CreateGroupConf groupConf = objectMapper.readValue(content, CreateGroupConf.class);
                // get the corresponding inlong group, aka the task to execute
                InlongClient inlongClient = ClientUtils.getClient();
                InlongGroup group = inlongClient.forGroup(groupConf.getGroupInfo());
                InlongStreamBuilder streamBuilder = group.createStream(groupConf.getStreamInfo());
                // put in parameters:source and sink,stream fields, then initialize
                streamBuilder.fields(groupConf.getStreamFieldList());
                streamBuilder.source(groupConf.getStreamSource());
                streamBuilder.sink(groupConf.getStreamSink());
                streamBuilder.initOrUpdate();
                // initialize the new stream group
                group.init();
                System.out.println("Create group success!");
            } catch (Exception e) {
                System.out.println("Create group failed!");
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Create cluster by json file")
    private static class CreateCluster extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-f", "--file"}, description = "json file", converter = FileConverter.class)
        private File file;

        @Override
        void run() {
            try {
                String content = ClientUtils.readFile(file);
                ClusterRequest request = objectMapper.readValue(content, ClusterRequest.class);
                ClientUtils.initClientFactory();
                InlongClusterClient clusterClient = ClientUtils.clientFactory.getClusterClient();
                Integer clusterId = clusterClient.saveCluster(request);
                if (clusterId != null) {
                    System.out.println("Create cluster success! ID: " + clusterId);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Create cluster tag by json file")
    private static class CreateClusterTag extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-f", "--file"}, description = "json file", converter = FileConverter.class)
        private File file;

        @Override
        void run() {
            try {
                String content = ClientUtils.readFile(file);
                ClusterTagRequest request = objectMapper.readValue(content, ClusterTagRequest.class);
                ClientUtils.initClientFactory();
                InlongClusterClient clusterClient = ClientUtils.clientFactory.getClusterClient();
                Integer tagId = clusterClient.saveTag(request);
                if (tagId != null) {
                    System.out.println("Create cluster tag success! ID: " + tagId);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Create cluster node by json file")
    private static class CreateClusterNode extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-f", "--file"}, description = "json file", converter = FileConverter.class)
        private File file;

        @Override
        void run() {
            try {
                String content = ClientUtils.readFile(file);
                ClusterNodeRequest request = objectMapper.readValue(content, ClusterNodeRequest.class);
                ClientUtils.initClientFactory();
                InlongClusterClient clusterClient = ClientUtils.clientFactory.getClusterClient();
                Integer nodeId = clusterClient.saveNode(request);
                if (nodeId != null) {
                    System.out.println("Create cluster node success! ID: " + nodeId);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
