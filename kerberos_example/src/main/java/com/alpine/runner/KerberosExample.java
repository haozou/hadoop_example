package com.alpine.runner;

import com.alpine.utility.Utilities;
import com.facebook.fb303.FacebookBase;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.Hadoop23Shims;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.thrift.meta_data.EnumMetaData;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hao on 8/19/15.
 */
public class KerberosExample extends Configured implements Tool {
    public static final List<Class> libjars = new ArrayList<Class>() {{
        add(HCatRecord.class); //hcatalog-core
        //add(HCatClient.class); //hcatalog-webclient
        //add(HiveEndPoint.class);
        //add(HCatEventMessage.class);
        add(HiveMetaStore.class);
        add(EnumMetaData.class);
        //add(AbstractMapJoinOperator.class);
        add(FacebookBase.class);
    }};

    public static void main(final String[] args) throws Exception {
        final String[] hadoopArgs = {"-libjars", Utilities.libjars(libjars)};
        final Configuration conf = new Configuration();
        conf.set("hive.metastore.sasl.enabled", "true");
        conf.set("hive.server2.enable.impersonation", "true");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("chorus/chorus.alpinenow.local@ALPINE", "/Users/Hao/workspace/hadoop_example/kerberos_example/chorus.keytab");
        //ugi = UserGroupInformation.createProxyUser("hive", ugi);
        ugi.doAs(new PrivilegedExceptionAction<KerberosExample>() {
            public KerberosExample run() throws Exception {
                KerberosExample mr = new KerberosExample();
                ToolRunner.run(conf, mr, (String[]) ArrayUtils.addAll(hadoopArgs, args));
                return mr;
            }
        });
        System.exit(0);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        for (FileStatus fileStatus : fs.listStatus(new Path("/"))) {
            System.err.println(fileStatus.getPath().getName());
        }
        /*HCatClient example code start*/
        HCatClient client = HCatClient.create(conf);
        for (String name : client.listTableNamesByPattern(null, "*")) {
            System.err.println(name);
        }
        return 0;
    }
}
