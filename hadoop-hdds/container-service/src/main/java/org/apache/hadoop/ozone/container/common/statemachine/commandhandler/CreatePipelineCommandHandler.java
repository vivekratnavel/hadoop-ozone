/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CreatePipelineCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CreatePipelineACKProto;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client
    .CertificateClient;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommandStatus;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Handler for create pipeline command received from SCM.
 */
public class CreatePipelineCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CreatePipelineCommandHandler.class);

  private AtomicLong invocationCount = new AtomicLong(0);
  private long totalTime;

  /**
   * Constructs a createPipelineCommand handler.
   */
  public CreatePipelineCommandHandler() {
  }

  /**
   * Handles a given SCM command.
   *
   * @param command           - SCM Command
   * @param ozoneContainer    - Ozone Container.
   * @param context           - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand command, OzoneContainer ozoneContainer,
      StateContext context, SCMConnectionManager connectionManager) {
    invocationCount.incrementAndGet();
    final long startTime = Time.monotonicNow();
    final DatanodeDetails dn = context.getParent()
        .getDatanodeDetails();
    final CreatePipelineCommandProto createCommand =
        ((CreatePipelineCommand)command).getProto();
    final PipelineID pipelineID = PipelineID.getFromProtobuf(
        createCommand.getPipelineID());
    Collection<DatanodeDetails> peers =
        createCommand.getDatanodeList().stream()
            .map(DatanodeDetails::getFromProtoBuf)
            .collect(Collectors.toList());

    final CreatePipelineACKProto createPipelineACK =
        CreatePipelineACKProto.newBuilder()
            .setPipelineID(createCommand.getPipelineID())
            .setDatanodeUUID(dn.getUuidString()).build();
    boolean success = false;
    try {
      createPipeline(dn, pipelineID, peers, context);
      success = true;
      LOG.info("Create Pipeline {} {} #{} command succeed on datanode {}.",
          createCommand.getType(), createCommand.getFactor(), pipelineID,
          dn.getUuidString());
    } catch (NotLeaderException e) {
      LOG.debug("Follower cannot create pipeline #{}.", pipelineID);
    } catch (IOException e) {
      LOG.error("Can't create pipeline {} {} #{}", createCommand.getType(),
          createCommand.getFactor(), pipelineID, e);
    } finally {
      final boolean cmdExecuted = success;
      Consumer<CommandStatus> statusUpdater = (cmdStatus) -> {
        cmdStatus.setStatus(cmdExecuted);
        ((CreatePipelineCommandStatus)cmdStatus)
            .setCreatePipelineAck(createPipelineACK);
      };
      updateCommandStatus(context, command, statusUpdater, LOG);
      long endTime = Time.monotonicNow();
      totalTime += endTime - startTime;
    }
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.createPipelineCommand;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return (int)invocationCount.get();
  }

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  @Override
  public long getAverageRunTime() {
    if (invocationCount.get() > 0) {
      return totalTime / invocationCount.get();
    }
    return 0;
  }

  /**
   * Sends ratis command to create pipeline on this datanode.
   *
   * @param dn       - this datanode
   * @param pipelineId   - pipeline ID
   * @param peers        - datanodes of the pipeline
   * @param context      - Ozone datanode context
   * @throws IOException
   */
  private void createPipeline(DatanodeDetails dn, PipelineID pipelineId,
      Collection<DatanodeDetails> peers, StateContext context)
      throws IOException {
    final Configuration ozoneConf = context.getParent().getConf();
    final RaftGroup group = RatisHelper.newRaftGroup(
        RaftGroupId.valueOf(pipelineId.getId()), peers);
    LOG.debug("creating pipeline:#{} with {}", pipelineId, group);

    final String rpcType = ozoneConf
        .get(ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneConf);
    final List< IOException > exceptions =
        Collections.synchronizedList(new ArrayList<>());
    final int maxOutstandingRequests =
        HddsClientUtils.getMaxOutstandingRequests(ozoneConf);
    final CertificateClient dnCertClient =
        context.getParent().getCertificateClient();
    final GrpcTlsConfig tlsConfig = RatisHelper.createTlsServerConfigForDN(
        new SecurityConfig(ozoneConf), dnCertClient);
    final TimeDuration requestTimeout =
        RatisHelper.getClientRequestTimeout(ozoneConf);
    try {
      final RaftPeer peer = RatisHelper.toRaftPeer(dn);
      try (RaftClient client = RatisHelper
          .newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType), peer,
              retryPolicy, maxOutstandingRequests, tlsConfig,
              requestTimeout)) {

        RaftClientReply reply = client.groupAdd(group, peer.getId());
        if (reply == null || !reply.isSuccess()) {
          String msg = "Pipeline initialization failed for pipeline:"
              + pipelineId.getId() + " node:" + peer.getId();
          throw new IOException(msg);
        }
      } catch (IOException ioe) {
        String errMsg =
            "Failed invoke Ratis rpc for " + dn.getUuid();
        exceptions.add(new IOException(errMsg, ioe));
      }
    } catch (RejectedExecutionException ex) {
      throw new IOException(ex.getClass().getName() + " exception occurred " +
          "during createPipeline", ex);
    }

    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }
}
