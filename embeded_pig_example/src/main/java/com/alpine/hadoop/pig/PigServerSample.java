package com.alpine.hadoop.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.security.PrivilegedExceptionAction;

public class PigServerSample {

    public static void main(final String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("hdfs");
        ugi.doAs(new PrivilegedExceptionAction<PigServerSample>() {
            public PigServerSample run() throws Exception {
                PigServerSample sample = new PigServerSample();
                sample.runPig(args);
                return sample;
            }
        });
    }
    public void runPig(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("fs.default.name", "hdfs://awshdp2regression.alpinenow.local:8020");

        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.application.classpath", "/etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*");
        conf.set("yarn.resourcemanager.address", "awshdp2regression.alpinenow.local:8050");
        conf.set("yarn.resourcemanager.scheduler.address", "awshdp2regression.alpinenow.local:8030");

        conf.set("hive.metastore.uris", "thrift://awshdp2regression.alpinenow.local:9083");

        conf.set("javax.jdo.option.ConnectionURL", "jdbc:mysql://awshdp2regression.alpinenow.local/hivemetastoredb?createDatabaseIfNotExist=true");

        conf.set("javax.jdo.option.ConnectionDriverName", "org.mysql.jdbc.Driver");
        conf.set("javax.jdo.option.ConnectionUserName", "hive");
        conf.set("javax.jdo.option.ConnectionPassword", "hive");
        PigServer server = new PigServer(ExecType.MAPREDUCE, conf);
        server.registerQuery("register 'hdfs://awshdp2regression.alpinenow.local:8020/tmp/*.jar';");
        server.registerQuery("A = load 'logs1' using org.apache.hcatalog.pig.HCatLoader();");
        // This store will generate a job without reducer task, it will work
        server.store("A", "hdfs://awshdp2regression.alpinenow.local:8020/tmp/result_aaaxa") ;

        server.registerQuery("B = limit A 3;");
        // This store will generate a job with reducer task, it will fail
        server.store("B", "hdfs://awshdp2regression.alpinenow.local:8020/tmp/result_b3xeb") ;
    }
}
