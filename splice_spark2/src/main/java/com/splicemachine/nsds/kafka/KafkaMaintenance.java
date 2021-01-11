/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.splicemachine.nsds.kafka;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.log4j.Logger;

public class KafkaMaintenance {

    private static Logger LOG = Logger.getLogger(KafkaMaintenance.class);

    public static AdminClient adminClient(String kafkaServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        return AdminClient.create( props );
    }
    
    public static void deleteOldTopics(
        String kafkaServers,
        String dataFileName,
        long ageCutoffMinutes,
        List<String> prefixToDelete,
        List<String> prefixToKeep
    ) throws Exception {
        LOG.info( "Instantiating Kafka client" );
        AdminClient admin = adminClient( kafkaServers );

        LOG.info( "Initializing variables" );
        Path dataFilePath = Paths.get(dataFileName);
        String delim = ",";
        Instant now = Instant.now();
        Set<String> undeleted = new HashSet<String>();

        if( Files.exists( dataFilePath ) ) {
            LOG.info( "Reading " + dataFilePath );
            List<String> savedTopics = Files.readAllLines(dataFilePath);
            
            LOG.info( "Finding old topics to delete" );
            Set<String> oldTopicNames = savedTopics.stream()
                .filter(ln -> Instant.ofEpochMilli(Long.parseLong(ln.split(delim)[1])).plus(ageCutoffMinutes, ChronoUnit.MINUTES).isBefore(now))
                .map(ln -> ln.split(delim)[0])
                .collect(Collectors.toSet());
            
            LOG.info( "Deleting old topics" );
            Collection<KafkaFuture<Void>> futures = admin.deleteTopics(oldTopicNames).values().values();
            for(KafkaFuture<Void> future : futures) {
                try {
                    future.get();
                } catch(java.util.concurrent.ExecutionException e) {
                    if (!e.getMessage().contains("org.apache.kafka.common.errors.UnknownTopicOrPartitionException")) {
                        LOG.warn( "Problem trying to delete a topic.\n" + e);
                    }
                }
            }

            LOG.info( "Getting topics not ready for delete" );
            undeleted.addAll( savedTopics.stream().filter(ln -> !oldTopicNames.contains(ln.split(delim)[0]))
                .collect(Collectors.toSet()) );
        }
        
        LOG.info( "Getting topic list from Kafka" );
        Set<String> topicNamesInKafka = admin.listTopics().names().get();
        
        LOG.info( "Identifying new topics" );
        Set<String> undeletedTopicNames = undeleted.stream().map(ln -> ln.split(delim)[0]).collect(Collectors.toSet());
        Set<String> newTopics = topicNamesInKafka.stream()
            .filter(t -> ! undeletedTopicNames.contains(t))
            .filter(t -> prefixToDelete == null ? true : prefixToDelete.stream().anyMatch(prefix -> t.startsWith(prefix)) )
            .filter(t -> prefixToKeep == null ? true : ! prefixToKeep.stream().anyMatch(prefix -> t.startsWith(prefix)) )
            .map(t -> t + delim + now.toEpochMilli() )
            .collect(Collectors.toSet());
        
        undeleted.addAll( newTopics );
        
        LOG.info( "Writing topics to " + dataFilePath );
        Files.write(dataFilePath, undeleted);
    }

    /**
     * 
     * @param args args[0] Kafka Bootstrap Servers; args[1] path to topics file.
     *             Optional: args[3] Topic age cutoff in minutes
     *             args[4] Comma-separated list of topic prefixes to delete, topics with other prefixes will not be deleted
     *             args[5] Comma-separated list of topic prefixes to keep, topics with other prefixes will be deleted
     */
    public static void main(String[] args) {
        try {
            if( args.length < 2 ) {
                System.out.println(
                    "Required params: Kafka Bootstrap Servers, Path to topics file."
                    + "  Optional: Topic age cutoff in minutes,"
                    + " Comma-separated list of topic prefixes to delete,"
                    + " Comma-separated list of topic prefixes to keep."
                );
                System.exit(1);
            }
            long ageCutoffMinutes = 24*60;
            if( args.length >= 3 ) {
                ageCutoffMinutes = Long.parseLong( args[2] );
            }
            List<String> prefixToDelete = null;
            if( args.length >= 4 ) {
                prefixToDelete = Arrays.asList( args[3].split(",") );
            }
            List<String> prefixToKeep = null;
            if( args.length >= 5 ) {
                prefixToKeep = Arrays.asList( args[4].split(",") );
            }

            KafkaMaintenance.deleteOldTopics( args[0] , args[1] , ageCutoffMinutes , prefixToDelete , prefixToKeep );
        } catch(Exception e) {
            System.out.println(e);
        }
    }
}
