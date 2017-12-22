package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import static java.util.Arrays.asList;

/**
 * Offer strategy when external storage is involved. Notice when compared to the OfferStrategyNormal, the OfferRule for
 * checking if enough storage space is no longer needed because external volumes size is managed externally (storage
 * array, Amazon EBS, etc).
 */
public class OfferStrategyExternalStorage extends OfferStrategy {

    public OfferStrategyExternalStorage(final Configuration configuration, ClusterState clusterState) {
        super(configuration, clusterState);
        // Offer rule lambda returns false, it accepts the offer.
        // Offer rule lambda returns true, it declines the offer.
        acceptanceRules = asList(
          new OfferRule("Host already running task", offer -> !configuration.getMesosMultipleTasksPerHost() && isHostAlreadyRunningTask(offer)),
          new OfferRule("Hostname is unresolveable", offer -> !isHostnameResolveable(offer.getHostname())),
          new OfferRule("First ES node is not responding", offer -> !isAtLeastOneESNodeRunning() && configuration.getMesosOfferWaitForRunning()),
          new OfferRule("Cluster size already fulfilled", offer -> clusterState.getTaskList().size() >= configuration.getElasticsearchNodes()),
          new OfferRule("Offer did not have 2 ports", offer -> !containsTwoPorts(offer.getResourcesList())),
          new OfferRule("The offer does not contain the user specified ports", offer -> !(containsUserSpecifiedPorts(offer.getResourcesList()) || configuration.getMesosOfferIgnorePorts())),
          new OfferRule("Offer did not have enough CPU resources", offer -> !isEnoughCPU(configuration, offer.getResourcesList())),
          new OfferRule("Offer did not have enough RAM resources", offer -> !isEnoughRAM(configuration, offer.getResourcesList()))
        );

    }

}
