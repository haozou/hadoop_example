package com.alpine.runner;

import com.alpine.hcatalog.HCatMapper;
import com.alpine.hcatalog.HCatReducer;
import com.alpine.utility.Utilities;
import com.facebook.fb303.FacebookBase;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.thrift.meta_data.EnumMetaData;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Hao on 8/19/15.
 */
public class KerberosExample extends Configured implements Tool {
    private static final String HIVE_METASTORE_SASL_ENABLED = "hive.metastore.sasl.enabled";
    private static final String HIVE_METASTORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";
    private static final String HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";
    public static final List<Class> libjars = new ArrayList<Class>() {{
        add(HCatRecord.class); //hcatalog-core
        //add(HCatClient.class); //hcatalog-webclient
        //add(HiveEndPoint.class);
        //add(HCatEventMessage.class);
        add(HiveMetaStore.class);
        add(EnumMetaData.class);
        //add(AbstractMapJoinOperator.class);
    }};
    public static void main(final String[] args) throws Exception {
        Boolean a = Pattern.matches("[0-9]+", "10");
        final String[] hadoopArgs = {"-libjars", Utilities.libjars(libjars)};
        final Configuration conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);

        String principle = System.getProperty("principle");
        String keytab = System.getProperty("keytab");
        String user = System.getProperty("user");
        SecurityInfo securityInfo = new AnnotatedSecurityInfo();
        SecurityUtil.setSecurityInfoProviders(securityInfo);
        System.out.println("principle: " + principle + ", keytab: " + keytab + ", user: " + user);
        UserGroupInformation.loginUserFromKeytab(principle, keytab);
        // proxy user here is not working
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());

        // if uncomment this line to use the login user, it works
        //ugi = UserGroupInformation.getLoginUser();
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
        System.out.println("*********" + conf.get("hive.metastore.execute.setugi"));

//        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://cdh5hakerberosnn.alpinenow.local:8020"), conf);
//        for (FileStatus fs : fileSystem.listStatus(new Path("/"))) {
//            System.err.println(fs.getPath().getName());
//        }
        /*hcatalog example*/
        HiveConf hiveConf = null;
        HCatClient hiveclient = null;
        hiveConf = new HiveConf(conf, HiveConf.class);
        // specified a thrift url
        hiveConf.set(HIVE_METASTORE_SASL_ENABLED, "true");
        hiveConf.set(HIVE_METASTORE_KERBEROS_PRINCIPAL, "hive/_HOST@"+System.getProperty("java.security.krb5.realm"));
        String protection = conf.get(HADOOP_RPC_PROTECTION,
                SaslRpcServer.QualityOfProtection.AUTHENTICATION.name()
                        .toLowerCase());
        hiveConf.set(HADOOP_RPC_PROTECTION, protection);
        hiveConf.set("hive.security.metastore.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider");
        hiveConf.set("hive.security.metastore.authenticator.manager", "org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator");
        hiveConf.set("hive.metastore.pre.event.listeners", "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener");
        hiveConf.set("hive.metastore.execute.setugi", "true");
        HiveMetaStoreClient metaStoreClient  = HCatUtil.getHiveClient(hiveConf);
        Table table = HCatUtil.getTable(metaStoreClient, "default", "golfnew").getTTable();
        System.out.println(table.getCreateTime());

        System.out.println("*********" + hiveConf.get("hive.metastore.execute.setugi"));
        hiveclient = HCatClient.create(hiveConf);
        System.out.println("hive default database:");
        for (String name : hiveclient.listTableNamesByPattern(null, "*")) {
            System.err.println(name);
        }

        // Get the input and output table names as arguments
        String inputTableName = args[0];
        String outputTableName = args[1];


        /*HCatClient example code start*/
        /*HCatClient client = HCatClient.create(conf);
        PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        typeInfo.setTypeName("int");
        cols.add(new HCatFieldSchema("id", typeInfo, "id columns"));
        cols.add(new HCatFieldSchema("value", typeInfo, "value columns"));
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(null, args[1], cols).build();
        System.err.println("dbname: " + client.getDatabase("default").getName());
        if (client.listTableNamesByPattern(null, args[1]).isEmpty()) {
            System.err.println(args[1] + " is not exists, create it");
            client.createTable(tableDesc);

        }

        for (String name : client.listTableNamesByPattern("default", "*")) {
            System.err.println(name);
        }*/
        /*HCatClient example code end*/


        // Assume the default database
        String dbName = null;
        Job job = Job.getInstance(conf, "HCatalogWordCount");

        job.setJar("/Users/Hao/workspace/hadoop_example/kerberos_example/target/kerberos_example-1.0.jar");

        job.setMapperClass(HCatMapper.class);
        job.setReducerClass(HCatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(IntWritable.class);

        /*Set the input table*/
        HCatInputFormat.setInput(job.getConfiguration(), dbName, inputTableName);
        job.setInputFormatClass(HCatInputFormat.class);
        /*HCatSchema inputSchema = HCatInputFormat.getTableSchema(job.getConfiguration());
        System.err.println("INFO: input schema is :" + inputSchema);
        System.err.println("INFO: input field is:" + inputSchema.getFieldNames());*/

		/*Set the output table*/
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outputTableName))) {
            fs.delete(new Path(outputTableName), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputTableName));
        /*HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName, outputTableName, null));
        job.setOutputFormatClass(HCatOutputFormat.class);
        HCatSchema outputSchema = HCatOutputFormat.getTableSchema(job.getConfiguration());
        System.err.println("INFO: output schema explicitly set for writing:" + outputSchema);
        HCatOutputFormat.setSchema(job, outputSchema);*/

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
