package ru.akhtyamovpavel.hobod;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;


public class HostIpUDF extends GenericUDF {
    private PrimitiveObjectInspector argumentZero;
    private PrimitiveObjectInspector argumentOne;
    private PrimitiveObjectInspector.PrimitiveCategory inputZeroType;
    private PrimitiveObjectInspector.PrimitiveCategory inputOneType;

    private String ip;
    private String mask;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2 || arguments[0] == null || arguments[1] == null) {
            throw new UDFArgumentException("Two arguments: ip and mask expected");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
            arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("HostIp should get string");
        }

        argumentZero = (PrimitiveObjectInspector) arguments[0];
        argumentOne = (PrimitiveObjectInspector) arguments[1];

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ip = ((StringObjectInspector) argumentZero).getPrimitiveJavaObject(arguments[0].get());
        mask = ((StringObjectInspector) argumentOne).getPrimitiveJavaObject(arguments[1].get());

        String[] octetsIp = ip.split("\\.");
        String[] octetsMask = mask.split("\\.");


        Integer[] octetsIpInt = new Integer[4];
        Integer[] octetsMaskInt = new Integer[4];

        StringBuilder outputIp = new StringBuilder();

        for (int i = 0; i < 4; ++i) {
            octetsIpInt[i] = Integer.parseInt(octetsIp[i]);
            octetsMaskInt[i] = Integer.parseInt(octetsMask[i]);
            outputIp.append(octetsIpInt[i] & octetsMaskInt[i]);
            if (i < 3) {
                outputIp.append('.');
            }
        }
        return new Text(outputIp.toString());
    }

    public String getDisplayString(String[] strings) {
        return "Error in udf";
    }
}
