package com.alpine.runner;

//import com.alpine.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hao on 6/4/15.
 */
public class App {
    public void run(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException{
        ClassLoader classLoader = App.class.getClassLoader();
        Class clazz = classLoader.loadClass("com.alpine.runner.App");
        System.out.println("com.alpine".split("\\.")[0]);

        String name = "com.alpine.runner.App";
        String[] names = name.split("\\.");
        System.out.println(name.indexOf(names[names.length - 1]));
        System.out.println(name.substring(0, name.indexOf(names[names.length - 1]) - 1));
        Class testClass = clazz.getClasses()[0];
        System.out.println(testClass.getCanonicalName());
        Object testd = testClass.newInstance();
        Method test = testd.getClass().getDeclaredMethod("test");
        test.setAccessible(true);
        test.invoke(testd);
        //System.out.println(test.getName() + "!!!!!!!!!");
        // Deserialize users from disk
        FsInput fsInput = null;
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://10.0.0.146:8020");
        //File file = new File("StudentActivity.snappy.avro");
        Path p = new Path("hdfs://10.0.0.146:8020/Datasets/avro/StudentActivity.snappy.avro");
        fsInput = new FsInput(p, conf);
        //System.out.println(file.getAbsolutePath());
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(fsInput, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
            user = dataFileReader.next(user);
            int size = user.getSchema().getFields().size();
            for (int i = 0; i < size; i++)
                System.out.print(user.get(i).toString() + ",");
            System.out.println();
        }
    }
    public static void main(final String[] args) throws IOException, InterruptedException {

        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("mapred");
        ugi.doAs(new PrivilegedExceptionAction<App>() {
            public App run() throws Exception {
                App sample = new App();
                sample.run(args);
                return sample;
            }
        });
    }
    public static class Test {
        public void test() {
            System.out.println("nimadabi");
        }
    }
}
