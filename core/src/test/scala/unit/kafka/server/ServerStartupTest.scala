/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.integration.KafkaServerTestHarness
import kafka.utils.TestUtils.createAdminClient
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig}
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

class ServerStartupTest extends KafkaServerTestHarness {
  val overridingProps = new Properties()

  var broker: KafkaBroker = _

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    overridingProps.put(KafkaConfig.BrokerIdProp, 0)
    TestUtils.createBrokerConfigs(1, zkConnectOrNull).map(KafkaConfig.fromProps(_, overridingProps))
  }


  @AfterEach
  override def tearDown(): Unit = {
    if (broker != null)
      broker.shutdown()
    super.tearDown()
  }

  @Test
  def testBrokerCreatesZKChroot(): Unit = {
    val brokerId = 0
    val zookeeperChroot = "/kafka-chroot-for-unittest"
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val zooKeeperConnect = props.get("zookeeper.connect")
    props.put("zookeeper.connect", zooKeeperConnect.toString + zookeeperChroot)
    val broker = createBroker(KafkaConfig.fromProps(props))
    val pathExists = zkClient.pathExists(zookeeperChroot)
    assertTrue(pathExists)
    broker.shutdown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConflictBrokerStartupWithSamePort(quorum: String): Unit = {
    val port = TestUtils.boundPort(brokers.head)

    // Create a second broker with same port
    val brokerId2 = 1
    val props2 = TestUtils.createBrokerConfig(brokerId2, zkConnectOrNull, port = port)
    if (isKRaftTest()) {
      assertThrows(classOf[RuntimeException], () => createBroker(KafkaConfig.fromProps(props2)))
    } else {
      assertThrows(classOf[IllegalArgumentException], () => createBroker(KafkaConfig.fromProps(props2)))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testRemoteStorageEnabled(quorum: String): Unit = {
    // Create and start first broker
    val brokerId1 = 1
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnectOrNull)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props1.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "badListenerName")
    assertThrows(classOf[ConfigException], () => createBroker(KafkaConfig.fromProps(props1)))
    // should not throw exception after adding a correct value for "remote.log.metadata.manager.listener.name"
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "PLAINTEXT")
    createBroker(KafkaConfig.fromProps(props1))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testRemoteStorageEnabledWithoutSettingListener(quorum: String): Unit = {
    // Create and start first broker
    val brokerId1 = 1
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnectOrNull)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props1.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    // should not throw exception if "remote.log.metadata.manager.listener.name" is unconfigured
    broker = createBroker(KafkaConfig.fromProps(props1))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConflictBrokerRegistration(quorum: String): Unit = {
    // Try starting a broker with the a conflicting broker id.
    // This shouldn't affect the existing broker registration.

    val brokerId = 0
    val admin = createAdminClient(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
//    val brokerRegistration = zkClient.getBroker(brokerId).getOrElse(fail("broker doesn't exists"))
    //0 : (EndPoint(localhost,56032,ListenerName(PLAINTEXT),PLAINTEXT)) : null : Features{}
    val brokerRegistration = admin.describeCluster().nodes().get()
    //    System.out.println(brokerRegistration)
    val props2 = TestUtils.createBrokerConfig(brokerId, zkConnectOrNull)
    assertThrows(classOf[NodeExistsException], () => createBroker(KafkaConfig.fromProps(props2)))

    // broker registration shouldn't change
    assertEquals(brokerRegistration, zkClient.getBroker(brokerId).getOrElse(fail("broker doesn't exists")))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testBrokerSelfAware(quorum: String): Unit = {
    TestUtils.waitUntilTrue(() => brokers.head.metadataCache.getAliveBrokers().nonEmpty, "Wait for cache to update")
    assertEquals(1, brokers.head.metadataCache.getAliveBrokers().size)
    assertEquals(0, brokers.head.metadataCache.getAliveBrokers().head.id)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testBrokerStateRunningAfterZK(quorum: String): Unit = {
    TestUtils.waitUntilTrue(() => brokers.head.brokerState == BrokerState.RUNNING,
      "waiting for the broker state to become RUNNING")
    assertEquals(1, brokers.size)
    assertEquals(0, brokers.head.config.brokerId)
  }
}
