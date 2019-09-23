/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CreatePipelineACKProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
/**
 * Command status to report about pipeline creation.
 */
public class CreatePipelineCommandStatus extends CommandStatus {

  private CreatePipelineACKProto createPipelineAck;

  public CreatePipelineCommandStatus(Type type, Long cmdId, Status status,
      String msg, CreatePipelineACKProto ack) {
    super(type, cmdId, status, msg);
    this.createPipelineAck = ack;
  }

  public void setCreatePipelineAck(CreatePipelineACKProto ack) {
    createPipelineAck = ack;
  }

  @Override
  public CommandStatus getFromProtoBuf(
      StorageContainerDatanodeProtocolProtos.CommandStatus cmdStatusProto) {
    return CreatePipelineCommandStatusBuilder.newBuilder()
        .setCreatePipelineAck(cmdStatusProto.getCreatePipelineAck())
        .setCmdId(cmdStatusProto.getCmdId())
        .setStatus(cmdStatusProto.getStatus())
        .setType(cmdStatusProto.getType())
        .setMsg(cmdStatusProto.getMsg())
        .build();
  }

  @Override
  public StorageContainerDatanodeProtocolProtos.CommandStatus
      getProtoBufMessage() {
    StorageContainerDatanodeProtocolProtos.CommandStatus.Builder builder =
        StorageContainerDatanodeProtocolProtos.CommandStatus.newBuilder()
            .setCmdId(this.getCmdId())
            .setStatus(this.getStatus())
            .setType(this.getType());
    if (createPipelineAck != null) {
      builder.setCreatePipelineAck(createPipelineAck);
    }
    if (this.getMsg() != null) {
      builder.setMsg(this.getMsg());
    }
    return builder.build();
  }

  /**
   * Builder for CreatePipelineCommandStatus.
   */
  public static final class CreatePipelineCommandStatusBuilder
      extends CommandStatusBuilder {
    private CreatePipelineACKProto createPipelineAck = null;

    public static CreatePipelineCommandStatusBuilder newBuilder() {
      return new CreatePipelineCommandStatusBuilder();
    }

    public CreatePipelineCommandStatusBuilder setCreatePipelineAck(
        CreatePipelineACKProto ack) {
      this.createPipelineAck = ack;
      return this;
    }

    @Override
    public CommandStatus build() {
      return new CreatePipelineCommandStatus(getType(), getCmdId(), getStatus(),
          getMsg(), createPipelineAck);
    }
  }
}
