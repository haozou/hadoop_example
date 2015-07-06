package com.alpine.hadoop.pig;

import com.alpine.utility.Utilities;
import com.facebook.fb303.FacebookBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.pig.HCatLoader;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import parquet.org.apache.thrift.meta_data.EnumMetaData;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

public class PigWithHcatalog extends Configured implements Tool {
    public static final List<Class> libjars = new ArrayList<Class>() {{
        add(HCatConstants.class); //hcatalog-core
        add(HCatLoader.class);
        //add(HCatClient.class); //hcatalog-webclient
        //add(HiveEndPoint.class);
        //add(HCatEventMessage.class);
        add(HiveMetaStore.class);
        add(EnumMetaData.class);
        //add(AbstractMapJoinOperator.class);
        add(FacebookBase.class);
    }};

    public static void main(final String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("mapred");
        ugi.doAs(new PrivilegedExceptionAction<PigWithHcatalog>() {
            public PigWithHcatalog run() throws Exception {
                PigWithHcatalog sample = new PigWithHcatalog();
                int res = ToolRunner.run(new Configuration(), sample, args);
                return sample;
            }
        });
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hive.metastore.uris", "thrift://cdh5cm.alpinenow.local:9083");

        PigServer server = new PigServer(ExecType.MAPREDUCE, conf);
        for (String jar : Utilities.findJarAbsPaths(libjars)) {
            server.registerJar(jar);
        }
        server.registerQuery("A = load " + "'" + args[0] + "'" + " using org.apache.hcatalog.pig.HCatLoader();");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
//        server.store("A", "hdfs://cdh5nn.alpinenow.local:8020" + args[1]);
//        if (fs.exists(new Path(args[1]))) {
//            fs.delete(new Path(args[1]), true);
//        }
        server.registerQuery("B = limit A 3;");
        server.store("B", "hdfs://cdh5nn.alpinenow.local:8020" + args[1]);
        return 0;
    }
}
