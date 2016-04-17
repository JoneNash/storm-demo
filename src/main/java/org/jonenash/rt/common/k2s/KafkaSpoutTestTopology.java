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
 * Ŀ��:storm���ѵ�ǰ����kafka����Ϣ,����ӡ
 * kafka��Ϣ:(127.0.0.1)��9092�˿�
 *
 * */
public class KafkaSpoutTestTopology {

    public static void main(String[] args) {
//��������kafkaspout
        String kafkaHost = "127.0.0.1";//kafka������IP
        Broker brokerForPartition0 = new Broker(kafkaHost, 9092);//���������˿�
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        partitionInfo.addPartition(0, brokerForPartition0);//
        StaticHosts hosts = new StaticHosts(partitionInfo);//kafka
        String topic="test";

        String offsetZkRoot ="/Users/leidelong/work/tools/offset/stormExample";//ƫ����offset�ĸ�Ŀ¼
        String offsetZkId="KafkaSpoutTest";//Ӧ�õ�Ŀ¼
        String offsetZkServers = "127.0.0.1";//zookeeper������IP
        String offsetZkPort = "2181";//zookeeper�˿�
        List<String> zkServersList = new ArrayList<String>();
        zkServersList.add(offsetZkServers);

        SpoutConfig kafkaConfig = new SpoutConfig(hosts,topic,offsetZkRoot,offsetZkId);

        kafkaConfig.zkPort = Integer.parseInt(offsetZkPort);
        kafkaConfig.zkServers = zkServersList;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());//����Ϊ���string����
        kafkaConfig.forceFromStart=true;//��ͷ����

        KafkaSpout spout = new KafkaSpout(kafkaConfig);


 //����storm����
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout, 1);
        builder.setBolt("bolt", new MyStormBolt(), 1).shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(false);//�Ƿ����debugģʽ,����ʱ�رոò���

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka2storm", config, builder.createTopology());

    }
}
