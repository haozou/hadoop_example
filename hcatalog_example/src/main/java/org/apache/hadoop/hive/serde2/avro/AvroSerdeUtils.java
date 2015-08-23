//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

public class AvroSerdeUtils {
    public static final String SCHEMA_LITERAL = "avro.schema.literal";
    public static final String SCHEMA_URL = "avro.schema.url";
    public static final String SCHEMA_NONE = "none";
    public static final String SCHEMA_NAMESPACE = "avro.schema.namespace";
    public static final String SCHEMA_NAME = "avro.schema.name";
    public static final String SCHEMA_DOC = "avro.schema.doc";
    public static final String EXCEPTION_MESSAGE = "Neither avro.schema.literal nor avro.schema.url specified, can\'t determine table schema";
    public static final String AVRO_SERDE_SCHEMA = "avro.serde.schema";
    private static final Log LOG = LogFactory.getLog(org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.class);

    public AvroSerdeUtils() {
    }

    public static Schema determineSchemaOrThrowException(Properties properties) throws IOException, AvroSerdeException {
        String schemaString = properties.getProperty("avro.schema.literal");
        if (schemaString != null && !schemaString.equals("none")) {
            return Schema.parse(schemaString);
        } else {
            schemaString = properties.getProperty("avro.schema.url");
            if (schemaString != null && !schemaString.equals("none")) {
                try {
                    /*alpine hack: we did this to make it possible to set Hadoop Configuration in runtime
                    * instead of reading configuration from the resources path*/
                    //Schema urie = getSchemaFromFS(schemaString, new Configuration());
                    Schema urie = getSchemaFromFS(schemaString, Hive.get().getConf());
                    return urie == null ? Schema.parse((new URL(schemaString)).openStream()) : urie;
                } catch (IOException var3) {
                    throw new AvroSerdeException("Unable to read schema from given path: " + schemaString, var3);
                } catch (URISyntaxException var4) {
                    throw new AvroSerdeException("Unable to read schema from given path: " + schemaString, var4);
                } catch (HiveException e) {
                    throw new AvroSerdeException(e);
                }
            } else {
                throw new AvroSerdeException("Neither avro.schema.literal nor avro.schema.url specified, can\'t determine table schema");
            }
        }
    }

    public static Schema determineSchemaOrReturnErrorSchema(Properties props) {
        try {
            return determineSchemaOrThrowException(props);
        } catch (AvroSerdeException var2) {
            LOG.warn("Encountered AvroSerdeException determining schema. Returning signal schema to indicate problem", var2);
            return SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
        } catch (Exception var3) {
            LOG.warn("Encountered exception determining schema. Returning signal schema to indicate problem", var3);
            return SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
        }
    }

    protected static Schema getSchemaFromFS(String schemaFSUrl, Configuration conf) throws IOException, URISyntaxException {
        FSDataInputStream in = null;
        FileSystem fs = null;

        try {
            fs = FileSystem.get(new URI(schemaFSUrl), conf);
        } catch (IOException var9) {
            String msg = "Failed to open file system for uri " + schemaFSUrl + " assuming it is not a FileSystem url";
            LOG.debug(msg, var9);
            return null;
        }

        Schema msg1;
        try {
            in = fs.open(new Path(schemaFSUrl));
            Schema s = Schema.parse(in);
            msg1 = s;
        } finally {
            if (in != null) {
                in.close();
            }

        }

        return msg1;
    }

    public static boolean isNullableType(Schema schema) {
        return schema.getType().equals(Type.UNION) && schema.getTypes().size() == 2 && (schema.getTypes().get(0).getType().equals(Type.NULL) || schema.getTypes().get(1).getType().equals(Type.NULL));
    }

    public static Schema getOtherTypeFromNullableType(Schema schema) {
        List types = schema.getTypes();
        return ((Schema) types.get(0)).getType().equals(Type.NULL) ? (Schema) types.get(1) : (Schema) types.get(0);
    }

    public static boolean insideMRJob(JobConf job) {
        return job != null && HiveConf.getVar(job, ConfVars.PLAN) != null && !HiveConf.getVar(job, ConfVars.PLAN).isEmpty();
    }

    public static Buffer getBufferFromBytes(byte[] input) {
        ByteBuffer bb = ByteBuffer.wrap(input);
        return bb.rewind();
    }

    public static Buffer getBufferFromDecimal(HiveDecimal dec, int scale) {
        if (dec == null) {
            return null;
        } else {
            dec = dec.setScale(scale);
            return getBufferFromBytes(dec.unscaledValue().toByteArray());
        }
    }

    public static byte[] getBytesFromByteBuffer(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        byte[] result = new byte[byteBuffer.limit()];
        byteBuffer.get(result);
        return result;
    }

    public static HiveDecimal getHiveDecimalFromByteBuffer(ByteBuffer byteBuffer, int scale) {
        byte[] result = getBytesFromByteBuffer(byteBuffer);
        HiveDecimal dec = HiveDecimal.create(new BigInteger(result), scale);
        return dec;
    }
}
