package org.jonenash.rt.common.k2s;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by leidelong on 16/4/13.
 */

/**
 * 目的:storm消费当前机器kafka的消息,并打印
 * kafka信息:(127.0.0.1)的9092端口
 *
 * */
public class KafkaSpoutTestTopology {

    public static void main(String[] args) {
//以下设置kafkaspout
        String kafkaHost = "127.0.0.1";//kafka服务器IP
        Broker brokerForPartition0 = new Broker(kafkaHost, 9092);//服务器及端口
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        partitionInfo.addPartition(0, brokerForPartition0);//
        StaticHosts hosts = new StaticHosts(partitionInfo);//kafka
        String topic="test";

        String offsetZkRoot ="/Users/leidelong/work/tools/offset/stormExample";//偏移量offset的根目录
        String offsetZkId="KafkaSpoutTest";//应用的目录
        String offsetZkServers = "127.0.0.1";//zookeeper服务器IP
        String offsetZkPort = "2181";//zookeeper端口
        List<String> zkServersList = new ArrayList<String>();
        zkServersList.add(offsetZkServers);

        SpoutConfig kafkaConfig = new SpoutConfig(hosts,topic,offsetZkRoot,offsetZkId);

        kafkaConfig.zkPort = Integer.parseInt(offsetZkPort);
        kafkaConfig.zkServers = zkServersList;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());//定义为输出string类型
        kafkaConfig.forceFromStart=true;//从头消费

        KafkaSpout spout = new KafkaSpout(kafkaConfig);


 //设置storm拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout, 1);
        builder.setBolt("bolt", new MyStormBolt(), 1).shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(false);//是否采用debug模式,上线时关闭该操作

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka2storm", config, builder.createTopology());

    }
}
