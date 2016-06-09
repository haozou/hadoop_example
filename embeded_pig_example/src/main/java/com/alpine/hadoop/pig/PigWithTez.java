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
import org.apache.pig.backend.hadoop.executionengine.tez.TezExecType;
import parquet.org.apache.thrift.meta_data.EnumMetaData;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hao on 4/19/16.
 */
public class PigWithTez extends Configured implements Tool {
    public static final List<Class> libjars = new ArrayList<Class>() {{

    }};
    public static void main(final String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("hdfs");
        ugi.doAs(new PrivilegedExceptionAction<PigWithTez>() {
            public PigWithTez run() throws Exception {
                PigWithTez sample = new PigWithTez();
                int res = ToolRunner.run(new Configuration(), sample, args);
                return sample;
            }
        });
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("tez.lib.uris", conf.get("fs.default.name") + "/tmp/tez.tar.gz");
        PigServer server = new PigServer(new TezExecType(), conf);
        for (String jar : Utilities.findJarAbsPaths(libjars)) {
            server.registerJar(jar);
        }
        server.registerQuery("A = load " + "'" + args[0] + "'" + " using PigStorage(\',\');");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        server.registerQuery("B = limit A 3;");
        server.store("B", "hdfs://hdp22a.alpinenow.local:8020" + args[1]);
        server.shutdown();
        return 0;
    }
}
