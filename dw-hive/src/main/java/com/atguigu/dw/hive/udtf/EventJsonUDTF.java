package com.atguigu.dw.hive.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2019/11/15 11:16
 */
public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        if (allStructFieldRefs.size() != 1) {
            throw new UDFArgumentException("参数个数只能是 1 ");
        }
        if (!"string".equals(allStructFieldRefs.get(0).getFieldObjectInspector().getTypeName())) {
            throw  new UDFArgumentException("参数类型必须是字符");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();  // 炸裂后的列名, 不重要, 用户一般会给每个列起别名

        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    /*

     */
    @Override
    public void process(Object[] args) throws HiveException {
        // 创建的那个json数组的字符串形式  [{}, {}]
        String et = args[0].toString();
        if (StringUtils.isNotBlank(et)) {
            try {
                JSONArray arr = new JSONArray(et);
                for (int i = 0; i < arr.length(); i++) {
                    JSONObject obj = arr.getJSONObject(i);
                    String en = obj.getString("en");
                    String kv = obj.getString("kv");

                    forward(new String[]{en, kv});
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }

        }

    }

    @Override
    public void close() throws HiveException {

    }
}
