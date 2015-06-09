package com.alpine.runner;

import com.alpine.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by Hao on 6/4/15.
 */
public class App {
    public static void main(final String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
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
        File file = new File("users.avro");
        System.out.println(file.getAbsolutePath());
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
            user = dataFileReader.next(user);
            int size = user.getSchema().getFields().size();
            for (int i = 0; i < size; i++)
                System.out.print(user.get(i) + ",");
            System.out.println();
        }
    }
    public static class Test {
        public void test() {
            System.out.println("nimadabi");
        }
    }
}
