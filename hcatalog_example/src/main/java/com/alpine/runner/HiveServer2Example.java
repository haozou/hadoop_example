package com.alpine.runner;
import com.alpine.utility.Utilities;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;

import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Arrays;

/**
 * Created by Hao on 5/19/16.
 */
public class HiveServer2Example {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("connName", "jdbc:hive2://cdh5hakerberosnn.alpinenow.local:10000/default;principal=chorus/chorus.alpinenow.local@ALPINE");
        UserGroupInformation.setConfiguration(conf);
        String principle = System.getProperty("principle");
        String keytab = System.getProperty("keytab");
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principle, keytab);
        ugi.doAs(new PrivilegedExceptionAction<HiveServer2Example>() {
            public HiveServer2Example run() throws Exception {
                //HiveServer2Example mr = new HiveServer2Example();
                test();
                return null;
            }
        });
        System.exit(0);
    }
    public static void test() throws Exception {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://cdh5hakerberosnn.alpinenow.local:10000/default;principal=hive/_HOST@ALPINE;hive.server2.proxy.user=hao");
        Statement stmt = con.createStatement();
        stmt.execute("use auto_test_data");
        stmt.execute("create table if not exists hao_test12 (`Id` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
                        "WITH SERDEPROPERTIES (\"casesensitive\"=\"Id\") " +
                        "STORED AS TEXTFILE");

        //stmt.execute("load data inpath '/tmp/alpine_out/hao/hive_kerberos/colfil_1/part-m-00000' OVERWRITE into table hao_test6");
        ResultSet res = stmt.executeQuery("show table extended like alp2_96_colfil_1");

        while (res.next()) {
            System.out.println(res.getString(1));
        }

        res = stmt.executeQuery("describe extended apple_customers");

        while (res.next()) {
            if (res.getString(1).contains("Detailed Table Information")) {
                for (String str : res.getString(2).replaceAll(".*Table\\((.*)\\).*", "$1").split(", ")) {
                    System.out.println(res.getString(2).replaceAll(".*Table\\((.*)\\).*", "$1"));
                }
            }
        }

        res = stmt.executeQuery("show databases");

        while (res.next()) {
            System.out.println(res.getString(1));
        }

        res = stmt.executeQuery("show tables");

        while (res.next()) {
            System.out.println(res.getString(1));
        }
        res.close();
        stmt.close();
        stmt = con.createStatement();
        res = stmt.executeQuery("describe golfnew");

        while (res.next()) {
            System.out.println(res.getString(2));
        }
        res.close();
        stmt.close();
        con.close();
    }
}
