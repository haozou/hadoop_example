package com.alpine.runner;

import com.facebook.fb303.FacebookBase;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.security.SaslRpcServer;
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
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String HIVE_METASTORE_SASL_ENABLED = "hive.metastore.sasl.enabled";
    private static final String HIVE_METASTORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";
    private static final String HIVE_METASTORE_LOCAL = "hive.metastore.local";
    private static final String HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);
        if (args.length < 3) {
            System.err.println("no principle, keytab or proxyuser provided");
            System.exit(1);
        }
        String principle = args[0];
        String keytab = args[1];
        String proxyUser = args[2];
        System.out.println("principle: " + principle + ", keytab: " + keytab + ", proxyUser: " + proxyUser);
        UserGroupInformation.loginUserFromKeytab(principle, keytab);
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(proxyUser, UserGroupInformation.getLoginUser());
        //ugi = UserGroupInformation.getLoginUser();
        ugi.doAs(new PrivilegedExceptionAction<KerberosExample>() {
            public KerberosExample run() throws Exception {
                KerberosExample mr = new KerberosExample();
                ToolRunner.run(conf, mr, args);
                return mr;
            }
        });
        System.exit(0);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        System.out.println("hdfs root directory:");
        for (FileStatus fileStatus : fs.listStatus(new Path("/"))) {
            System.err.println(fileStatus.getPath().getName());
        }

        /*hcatalog example*/
        HiveConf hiveConf = null;
        HCatClient hiveclient = null;
        hiveConf = new HiveConf();
        // specified a thrift url
        hiveConf.set(HIVE_METASTORE_SASL_ENABLED, "true");
        hiveConf.set(HIVE_METASTORE_KERBEROS_PRINCIPAL, "hive/_HOST@"+System.getProperty("java.security.krb5.realm"));
        String protection = conf.get(HADOOP_RPC_PROTECTION,
                SaslRpcServer.QualityOfProtection.AUTHENTICATION.name()
                        .toLowerCase());
        hiveConf.set(HADOOP_RPC_PROTECTION, protection);

        hiveclient = HCatClient.create(hiveConf);
        System.out.println("hive default database:");
        for (String name : hiveclient.listTableNamesByPattern(null, "*")) {
            System.err.println(name);
        }
        return 0;
    }
}
