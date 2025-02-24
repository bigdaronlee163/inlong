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

package org.apache.inlong.manager.plugin.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.plugin.flink.FlinkOperation;
import org.apache.inlong.manager.plugin.flink.FlinkService;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.plugin.flink.enums.Constants;
import org.apache.inlong.manager.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.manager.plugin.util.FlinkUtils.getExceptionStackMsg;

/**
 * Listener for startup stream sort
 */
@Slf4j
public class StartupStreamListener implements SortOperateListener {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext workflowContext) {
        ProcessForm processForm = workflowContext.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof StreamResourceProcessForm)) {
            log.info("not add startup stream listener, not StreamResourceProcessForm for groupId [{}]", groupId);
            return false;
        }

        StreamResourceProcessForm streamProcessForm = (StreamResourceProcessForm) processForm;
        String streamId = streamProcessForm.getStreamInfo().getInlongStreamId();
        if (streamProcessForm.getGroupOperateType() != GroupOperateType.INIT) {
            log.info("not add startup stream listener, as the operate was not INIT for groupId [{}] streamId [{}]",
                    groupId, streamId);
            return false;
        }

        log.info("add startup stream listener for groupId [{}] streamId [{}]", groupId, streamId);
        return true;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        ProcessForm processForm = context.getProcessForm();
        StreamResourceProcessForm streamResourceProcessForm = (StreamResourceProcessForm) processForm;
        InlongGroupInfo groupInfo = streamResourceProcessForm.getGroupInfo();
        List<InlongGroupExtInfo> groupExtList = groupInfo.getExtList();
        log.info("inlong group :{} ext info: {}", groupInfo.getInlongGroupId(), groupExtList);
        InlongStreamInfo streamInfo = streamResourceProcessForm.getStreamInfo();
        List<InlongStreamExtInfo> streamExtList = streamInfo.getExtList();
        log.info("inlong stream :{} ext info: {}", streamInfo.getInlongStreamId(), streamExtList);
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();

        if (CollectionUtils.isEmpty(streamResourceProcessForm.getStreamInfo().getSinkList())) {
            log.warn("not any sink configured for group {} and stream {}, skip launching sort job", groupId, streamId);
            return ListenerResult.success();
        }

        Map<String, String> kvConf = new HashMap<>();
        groupExtList.forEach(groupExtInfo -> kvConf.put(groupExtInfo.getKeyName(), groupExtInfo.getKeyValue()));
        streamExtList.forEach(extInfo -> {
            kvConf.put(extInfo.getKeyName(), extInfo.getKeyValue());
        });
        String sortExt = kvConf.get(InlongConstants.SORT_PROPERTIES);
        if (StringUtils.isNotEmpty(sortExt)) {
            Map<String, String> result = OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree(sortExt),
                    new TypeReference<Map<String, String>>() {
                    });
            kvConf.putAll(result);
        }

        String dataflow = kvConf.get(InlongConstants.DATAFLOW);
        if (StringUtils.isEmpty(dataflow)) {
            String message = String.format("dataflow is empty for groupId [%s] and streamId [%s]", groupId, streamId);
            log.error(message);
            return ListenerResult.fail(message);
        }

        FlinkInfo flinkInfo = new FlinkInfo();
        String jobName = Constants.INLONG + context.getProcessForm().getInlongGroupId();
        flinkInfo.setJobName(jobName);
        String sortUrl = kvConf.get(InlongConstants.SORT_URL);
        flinkInfo.setEndpoint(sortUrl);

        FlinkService flinkService = new FlinkService(flinkInfo.getEndpoint());
        FlinkOperation flinkOperation = new FlinkOperation(flinkService);

        try {
            flinkOperation.genPath(flinkInfo, dataflow);
            flinkOperation.start(flinkInfo);
            log.info("job submit success, jobId is [{}]", flinkInfo.getJobId());
        } catch (Exception e) {
            flinkOperation.pollJobStatus(flinkInfo);
            flinkInfo.setException(true);
            flinkInfo.setExceptionMsg(getExceptionStackMsg(e));
            flinkOperation.pollJobStatus(flinkInfo);

            String message = String.format("startup sort failed for groupId [%s] streamId [%s]", groupId, streamId);
            log.error(message, e);
            return ListenerResult.fail(message + e.getMessage());
        }

        saveInfo(groupId, streamId, InlongConstants.SORT_JOB_ID, flinkInfo.getJobId(), streamExtList);
        flinkOperation.pollJobStatus(flinkInfo);
        return ListenerResult.success();
    }

    /**
     * Save ext info into list.
     */
    private void saveInfo(String inlongGroupId, String inlongStreamId, String keyName, String keyValue,
            List<InlongStreamExtInfo> extInfoList) {
        InlongStreamExtInfo extInfo = new InlongStreamExtInfo();
        extInfo.setInlongGroupId(inlongGroupId);
        extInfo.setInlongStreamId(inlongStreamId);
        extInfo.setKeyName(keyName);
        extInfo.setKeyValue(keyValue);
        extInfoList.add(extInfo);
    }

}
