package com.alpine.runner;

import com.alpine.utility.Utilities;
import com.facebook.fb303.FacebookBase;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.thrift.meta_data.EnumMetaData;

import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * Created by Hao on 7/9/15.
 */
public class HCatalogExample extends Configured implements Tool {
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

        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("mapred");
        ugi.doAs(new PrivilegedExceptionAction<HCatalogExample>() {
            public HCatalogExample run() throws Exception {
                HCatalogExample mr = new HCatalogExample();
                ToolRunner.run(new Configuration(), mr, (String[]) ArrayUtils.addAll(hadoopArgs, args));
                return mr;
            }
        });
        System.exit(0);
    }

    public int run(String[] args) throws Exception {
        String[] names = args[0].split("\\.");
        ReadEntity.Builder builder = new ReadEntity.Builder();
        ReadEntity entity = builder.withDatabase(names[0]).withTable(names[1]).build();
        Configuration conf = new Configuration();
        //conf.addResource(new Path("/Users/Hao/workspace/hadoop_example/hcatalog_example/src/main/java/com/alpine/hadoop/hcatalog/core-site.xml"));
        conf.reloadConfiguration();
        conf.set("fs.defaultFS", "hdfs://10.10.2.36:8020");
        conf.set("hive.metastore.client.connect.retry.delay", "1");
        conf.set("hive.metastore.client.socket.timeout", "600");
        conf.set("hive.metastore.uris", "thrift://cdh5-automation1.alpinenow.local:9083");
        URL uri = this.getClass().getClassLoader().getSystemResource("core-site.xml");
//        HCatClient client = HCatClient.create(conf);
//        try {
//            HCatTable table = client.getTable("default", "hao_view");
//            System.out.println(table.getLocation());
//        } catch (HCatException e) {
//            System.out.println(e.getCause().getMessage());
//            if (e.getCause().getMessage().contains("There is no database named hao_view")) {
//                System.out.println("nimabi");
//            }
//        }

        Map<String, String> config = new HashMap<String, String>();
        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            config.put(entry.getKey(), entry.getValue());
        }

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("fs.defaultFS", "hdfs://10.10.2.36:8020");
        Hive.get(hiveConf);
        HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
        ReaderContext cntxt = reader.prepareRead();

        for (int split = 0; split < cntxt.numSplits(); split++) {
            HCatReader read = DataTransferFactory.getHCatReader(cntxt, split);
            Iterator<HCatRecord> itr = read.read();
            while (itr.hasNext()) {
                HCatRecord value = itr.next();
                int numOfCols = value.size();
                for (int i = 0; i < numOfCols; i++) {
                    System.out.print(value.get(i) + " ");
                }
                System.out.println();
            }
        }
        return 1;
        /*HCatClient example code start*/
        /*PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        typeInfo.setTypeName("double");
        int rows = 460;
        for (int i = 0; i < rows; i++) {
            cols.add(new HCatFieldSchema("col_" + Integer.toString(i), typeInfo, "id columns " + Integer.toString(i)));
        }
        HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create("default").ifNotExists(true).build();
        client.createDatabase(dbDesc);

        client.dropTable(null, "largedata", true);
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(null, "largedata", cols).ifNotExists(true).build();
        client.createTable(tableDesc);
        WriteEntity.Builder writerbuilder = new WriteEntity.Builder();
        WriteEntity writerEntity = writerbuilder.withDatabase("default").withTable("largedata").build();

        HCatWriter writer = DataTransferFactory.getHCatWriter(writerEntity, config);
        WriterContext writerContext = writer.prepareWrite();


        HCatWriter write = DataTransferFactory.getHCatWriter(writerContext);
        FileInputStream fis = new FileInputStream(new File("/Users/Hao/workspace/hadoop_example/hcatalog_example/equifax_data_gauss0.csv"));

        //Construct BufferedReader from InputStreamReader
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        String line = null;
        List<HCatRecord> list = new ArrayList<HCatRecord>();
//        while ((line = br.readLine()) != null) {
//            String[] row = line.split(",");
//            HCatRecord record = new DefaultHCatRecord(rows);
//            for (int i = 0; i < rows; i++) {
//                record.set(i, Double.parseDouble(row[i]));
//            }
//            list.add(record);
//
//            if (list.size() > 1) {
//                write.write(list.iterator());
//                //writer.commit(writerContext);
//                list.clear();
//            }
//
//        }
//        if (!list.isEmpty()) {
//            write.write(list.iterator());
//            writer.commit(writerContext);
//        }
        //writer.commit(writerContext);

        HCatRecordItr itr = new HCatRecordItr(br);
        write.write(itr);
        writer.commit(writerContext);
        br.close();



        return 1;
    }

    private static class HCatRecordItr implements Iterator<HCatRecord> {

        String line;
        BufferedReader br;
        public HCatRecordItr(BufferedReader br) {
            this.br = br;
        }
        @Override
        public boolean hasNext() {
            try {
                line = br.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (line == null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return line != null;
        }

        @Override
        public HCatRecord next() {
            String[] row = line.split(",");
            HCatRecord record = new DefaultHCatRecord(row.length);
            for (int i = 0; i < row.length; i++) {
                record.set(i, Double.parseDouble(row[i]));
            }
            return record;
        }

        @Override
        public void remove() {
            throw new RuntimeException();
        }*/
    }
}
