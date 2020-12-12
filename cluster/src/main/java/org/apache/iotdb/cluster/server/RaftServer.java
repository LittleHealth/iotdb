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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.cluster.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RaftServer works as a broker (network and protocol layer) that sends the requests to the proper
 * RaftMembers to process.
 */
public abstract class RaftServer implements RaftService.AsyncIface, RaftService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);
  private static int connectionTimeoutInMS =
      ClusterDescriptor.getInstance().getConfig().getConnectionTimeoutInMS();
  private static int readOperationTimeoutMS =
      ClusterDescriptor.getInstance().getConfig().getReadOperationTimeoutMS();
  private static int writeOperationTimeoutMS =
      ClusterDescriptor.getInstance().getConfig().getWriteOperationTimeoutMS();
  private static int syncLeaderMaxWaitMs = 20 * 1000;
  private static long[] heartBeatIntervalArray = {
          1000, 1773, 1671, 1000, 2229, 1250, 2688, 2130, 2034, 1000, 1048, 2185, 1343, 1182, 1016, 2921, 1377, 1335, 1000, 2639, 1132, 1683, 2263, 1000, 1170, 1000, 1000, 1812, 1652, 1000, 1000, 1776, 1703, 1000, 1507, 1000, 1000, 1073, 1943, 1207, 2487, 1991, 1533, 1000, 1000, 1230, 1213, 1000, 1478, 1000, 1923, 1055, 1783, 1000, 1326, 1901, 1954, 1000, 1000, 2621, 1875, 1000, 1000, 1000, 1224, 1000, 1666, 1778, 1640, 1516, 1525, 1036, 1204, 2622, 2173, 1994, 1854, 1091, 1118, 1000, 2752, 1108, 1169, 1706, 1260, 2811, 1500, 1762, 2100, 1832, 2348, 1947, 1000, 1104, 1872, 1785, 1480, 1287, 1940,
          40000,
          2081, 2380, 1000, 1714, 1561, 1659, 1290, 1490, 2356, 1000, 1878, 2719, 1000, 2155, 1682, 1000, 1146, 1343, 1379, 1271, 1871, 1000, 1000, 1715, 1000, 2454, 1667, 1394, 1170, 1627, 1282, 1485, 1000, 2232, 1410, 1312, 2145, 2121, 1271, 2191, 1000, 1185, 1485, 1000, 2142, 1113, 2142, 1184, 1650, 1000, 1339, 1958, 1530, 1682, 1000, 2651, 1409, 1929, 1290, 1000, 1183, 1762, 1488, 1183, 1000, 1851, 1561, 2518, 1072, 1086, 1000, 1211, 1000, 1622, 2257, 1767, 1000, 1266, 2619, 1000, 1000, 2101, 1000, 2431, 2263, 1454, 2297, 1000, 1742, 2105, 1863, 2686, 1603, 1837, 1000, 1000, 1964, 1000, 2958,
          40000,
  };
  private static int index = 0;
  private static long heartBeatIntervalMs = 1000L;

  ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  // the socket poolServer will listen to
  private TServerTransport socket;
  // RPC processing server
  private TServer poolServer;
  Node thisNode;

  TProtocolFactory protocolFactory = config.isRpcThriftCompressionEnabled() ?
      new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();

  // this thread pool is to run the thrift server (poolServer above)
  private ExecutorService clientService;

  RaftServer() {
    thisNode = new Node();
    thisNode.setIp(config.getClusterRpcIp());
    thisNode.setMetaPort(config.getInternalMetaPort());
    thisNode.setDataPort(config.getInternalDataPort());
    thisNode.setClientPort(config.getClusterRpcPort());
  }

  RaftServer(Node thisNode) {
    this.thisNode = thisNode;
  }

  public static int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public static void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    RaftServer.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public static int getReadOperationTimeoutMS() {
    return readOperationTimeoutMS;
  }

  public static int getWriteOperationTimeoutMS() {
    return writeOperationTimeoutMS;
  }

  public static int getSyncLeaderMaxWaitMs() {
    return syncLeaderMaxWaitMs;
  }

  public static void setSyncLeaderMaxWaitMs(int syncLeaderMaxWaitMs) {
    RaftServer.syncLeaderMaxWaitMs = syncLeaderMaxWaitMs;
  }
  public static long getHeartBeatIntervalMs() {
    heartBeatIntervalMs = heartBeatIntervalArray[index];
    index = (index+1)%200;
    return heartBeatIntervalMs;
  }

/*  public static long getHeartBeatIntervalMs() {
    return heartBeatIntervalMs;
  }*/

  public static void setHeartBeatIntervalMs(long heartBeatIntervalMs) {
    RaftServer.heartBeatIntervalMs = heartBeatIntervalMs;
  }

  /**
   * Establish a thrift server with the configurations in ClusterConfig to listen to and respond to
   * thrift RPCs. Calling the method twice does not induce side effects.
   *
   * @throws TTransportException
   */
  @SuppressWarnings("java:S1130") // thrown in override method
  public void start() throws TTransportException, StartupException {
    if (poolServer != null) {
      return;
    }

    establishServer();
  }

  /**
   * Stop the thrift server, close the socket and interrupt all in progress RPCs. Calling the method
   * twice does not induce side effects.
   */
  public void stop() {
    if (poolServer == null) {
      return;
    }

    poolServer.stop();
    socket.close();
    clientService.shutdownNow();
    socket = null;
    poolServer = null;
  }

  /**
   * @return An AsyncProcessor that contains the extended interfaces of a non-abstract subclass of
   * RaftService (DataService or MetaService).
   */
  abstract TProcessor getProcessor();

  /**
   * @return A socket that will be used to establish a thrift server to listen to RPC requests.
   * DataServer and MetaServer use different port, so this is to be determined.
   * @throws TTransportException
   */
  abstract TServerTransport getServerSocket() throws TTransportException;

  /**
   * Each thrift RPC request will be processed in a separate thread and this will return the name
   * prefix of such threads. This is used to fast distinguish DataServer and MetaServer in the logs
   * for the sake of debug.
   *
   * @return name prefix of RPC processing threads.
   */
  abstract String getClientThreadPrefix();

  /**
   * The thrift server will be run in a separate thread, and this will be its name. It help you
   * locate the desired logs quickly when debugging.
   *
   * @return The name of the thread running the thrift server.
   */
  abstract String getServerClientName();

  private TServer getAsyncServer() throws TTransportException {
    socket = getServerSocket();
    TThreadedSelectorServer.Args poolArgs =
        new TThreadedSelectorServer.Args((TNonblockingServerTransport) socket);
    poolArgs.maxReadBufferBytes =  IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize();
    poolArgs.selectorThreads(CommonUtils.getCpuCores());
    int maxConcurrentClientNum = Math.max(CommonUtils.getCpuCores(),
            config.getMaxConcurrentClientNum());
    poolArgs.executorService(new ThreadPoolExecutor(CommonUtils.getCpuCores(),
            maxConcurrentClientNum, poolArgs.getStopTimeoutVal(), poolArgs.getStopTimeoutUnit(),
        new SynchronousQueue<>(), new ThreadFactory() {
      private AtomicLong threadIndex = new AtomicLong(0);

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, getClientThreadPrefix() + threadIndex.incrementAndGet());
      }
    }));
    poolArgs.processor(getProcessor());
    poolArgs.protocolFactory(protocolFactory);
    // async service requires FramedTransport
    poolArgs.transportFactory(new TFastFramedTransport.Factory(
        IoTDBDescriptor.getInstance().getConfig().getThriftInitBufferSize(),
        IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize()));

    // run the thrift server in a separate thread so that the main thread is not blocked
    return new TThreadedSelectorServer(poolArgs);
  }

  private TServer getSyncServer() throws TTransportException {
    socket = getServerSocket();
    return ClusterUtils.createTThreadPoolServer(socket, getClientThreadPrefix(), getProcessor(),
        protocolFactory);
  }

  private void establishServer() throws TTransportException {
    logger.info("Cluster node {} begins to set up", thisNode);

    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      poolServer = getAsyncServer();
    } else {
      poolServer = getSyncServer();
    }

    clientService = Executors.newSingleThreadExecutor(r -> new Thread(r, getServerClientName()));
    clientService.submit(() -> poolServer.serve());

    logger.info("Cluster node {} is up", thisNode);
  }

  @TestOnly
  public static void setReadOperationTimeoutMS(int readOperationTimeoutMS) {
    RaftServer.readOperationTimeoutMS = readOperationTimeoutMS;
  }

  @TestOnly
  public static void setWriteOperationTimeoutMS(int writeOperationTimeoutMS) {
    RaftServer.writeOperationTimeoutMS = writeOperationTimeoutMS;
  }
}
