package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests
 **/
@SuppressWarnings("PMD.TooManyMethods")
public class ConfigurationTest {
    @Test
    public void shouldReturnValidServerPath() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        String localhost = "localhost";
        int port = 1234;
        configuration.setFrameworkFileServerAddress(new InetSocketAddress(localhost, port));
        assertEquals("http://" + localhost + ":" + port, configuration.getFrameworkFileServerAddress());
    }

    @Test
    public void shouldNotHaveDefaultInetAddressToStringMethod() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        int port = 1234;
        configuration.setFrameworkFileServerAddress(new InetSocketAddress(InetAddress.getLocalHost().getHostName(), port));
        assertFalse(configuration.getFrameworkFileServerAddress().replace("http://", "").contains("/"));
    }

    @Test
    public void shouldProvideJavaHomeWithEndSlashAndWithoutJava() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.JAVA_HOME, "/usr/bin/java");
        assertEquals("/usr/bin/", configuration.getJavaHome());
        configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.JAVA_HOME, "/usr/bin/");
        assertEquals("/usr/bin/", configuration.getJavaHome());
        configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.JAVA_HOME, "/usr/bin");
        assertEquals("/usr/bin/", configuration.getJavaHome());
    }

    @Test
    public void shouldGenerateValidNativeCommand() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        final List<String> arguments = Arrays.asList("test1", "test2");

        final String nativeCommand = configuration.nativeCommand(arguments);
        assertTrue(nativeCommand.contains(arguments.get(0)));
        assertTrue(nativeCommand.contains(arguments.get(1)));
        assertTrue(nativeCommand.contains("bin/elasticsearch"));
        assertTrue(nativeCommand.contains("chown"));
    }

    @Test
    public void shouldCreateArguments() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        final ClusterState clusterState = Mockito.mock(ClusterState.class);
        final int port = 1234;
        final Protos.DiscoveryInfo discoveryInfo = Protos.DiscoveryInfo.newBuilder().setPorts(Protos.Ports.newBuilder()
          .addPorts(Protos.Port.newBuilder().setNumber(port))
          .addPorts(Protos.Port.newBuilder().setNumber(port)))
          .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL)
          .build();
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue("SLAVE").build();
        final List<String> arguments = configuration.esArguments(clusterState, discoveryInfo, slaveID);
        String allArgs = arguments.toString();
        assertTrue(allArgs.contains(Integer.toString(port)));
    }

    @Test
    public void shouldCreateArgumentsWithHostname() {
        final String discoveryHostname = "ahostnameidefined";
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.DISCOVERY_ZEN_PING_UNICAST_HOSTS, discoveryHostname);
        final ClusterState clusterState = Mockito.mock(ClusterState.class);
        final int port = 1234;
        final Protos.DiscoveryInfo discoveryInfo = Protos.DiscoveryInfo.newBuilder().setPorts(Protos.Ports.newBuilder()
          .addPorts(Protos.Port.newBuilder().setNumber(port))
          .addPorts(Protos.Port.newBuilder().setNumber(port)))
          .setVisibility(Protos.DiscoveryInfo.Visibility.EXTERNAL)
          .build();
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue("SLAVE").build();
        final List<String> arguments = configuration.esArguments(clusterState, discoveryInfo, slaveID);
        String allArgs = arguments.toString();
        assertTrue(allArgs.contains(discoveryHostname));
    }

    @Test
    public void shouldCreateVolumeName() {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.FRAMEWORK_NAME, "test");
        assertEquals("test0data", configuration.dataVolumeName(0L));
    }

    @Test
    public void shouldSetIgnorePortsTrue() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_OFFER_IGNORE_PORTS, "true");
        assertTrue(configuration.getMesosOfferIgnorePorts());
    }

    @Test
    public void shouldSetIgnorePortsFalse() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_OFFER_IGNORE_PORTS, "false");
        assertFalse(configuration.getMesosOfferIgnorePorts());
    }

    @Test
    public void shouldSetIgnorePortsDefaultFalse() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        assertFalse(configuration.getMesosOfferIgnorePorts());
    }

    @Test
    public void shouldSetMesosDockerNetworkHost() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_DOCKER_NETWORK, "host");
        assertEquals(configuration.getTaskDockerNetworkProtos(), Protos.ContainerInfo.DockerInfo.Network.HOST);
    }

    @Test
    public void shouldSetMesosDockerNetworkBridge() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_DOCKER_NETWORK, "BRIDGE");
        assertEquals(configuration.getTaskDockerNetworkProtos(), Protos.ContainerInfo.DockerInfo.Network.BRIDGE);
    }

    @Test
    public void shouldSetMesosDockerNetworkUser() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_DOCKER_NETWORK, "USER");
        assertEquals(configuration.getTaskDockerNetworkProtos(), Protos.ContainerInfo.DockerInfo.Network.USER);
    }
    @Test
    public void shouldSetMesosDockerNetworkNone() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_DOCKER_NETWORK, "NONE");
        assertEquals(configuration.getTaskDockerNetworkProtos(), Protos.ContainerInfo.DockerInfo.Network.NONE);
    }

    @Test
    public void shouldSetMesosDockerNetworkDefault() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        assertEquals(configuration.getTaskDockerNetworkProtos(), Protos.ContainerInfo.DockerInfo.Network.HOST);

        configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_DOCKER_NETWORK, "notreal");
        assertEquals(configuration.getTaskDockerNetworkProtos(), Protos.ContainerInfo.DockerInfo.Network.HOST);
    }

    @Test
    public void shouldSetMesosDockerNetworkBadInput() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_DOCKER_NETWORK, "notreal");
        assertEquals(configuration.getTaskDockerNetworkProtos(), Protos.ContainerInfo.DockerInfo.Network.HOST);
    }

    @Test
    public void shouldNotSetMesosNetworkInfo() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_NETWORK_NAME, "");
        assertFalse(configuration.getNetworkInfo().isPresent());
    }

    @Test
    public void shouldNotSetMesosNetworkInfoDefault() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        assertFalse(configuration.getNetworkInfo().isPresent());
    }

    @Test
    public void shouldSetMesosNetworkInfo() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_TASK_NETWORK_NAME, "i'm a name!");
        assertTrue(configuration.getNetworkInfo().isPresent());
        assertEquals("i'm a name!", configuration.getNetworkInfo().get().getName());
    }

    @Test
    public void shouldSetWaitForRunningTrue() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_OFFER_WAIT_FOR_RUNNING, "true");
        assertTrue(configuration.getMesosOfferWaitForRunning());
    }

    @Test
    public void shouldSetWaitForRunningFalse() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_OFFER_WAIT_FOR_RUNNING, "false");
        assertFalse(configuration.getMesosOfferWaitForRunning());
    }

    @Test
    public void shouldSetWaitForRunningDefaultTrue() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        assertTrue(configuration.getMesosOfferWaitForRunning());
    }

    @Test
    public void shouldSetMultipleTasksTrue() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_MULTIPLE_TASKS_PER_HOST, "true");
        assertTrue(configuration.getMesosMultipleTasksPerHost());
    }

    @Test
    public void shouldSetMultipleTasksFalse() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa", Configuration.MESOS_MULTIPLE_TASKS_PER_HOST, "false");
        assertFalse(configuration.getMesosMultipleTasksPerHost());
    }

    @Test
    public void shouldSetMultipleTasksDefaultFalse() throws UnknownHostException {
        Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "aa");
        assertFalse(configuration.getMesosMultipleTasksPerHost());
    }

}
