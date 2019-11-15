package com.atguigu.dw.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @Author lzc
 * @Date 2019/11/15 10:41
 */
public class BaseFieldUdf extends UDF {
    /*
        udf
        参数: 1. ods中的一行数据 其实是一个不完整的json
        参数2: 需要解析出来的具体的key
     */
    public String evaluate(String line, String key) throws JSONException {
        if (line == null || line.length() == 0) {
            return "";
        }

        String[] split = line.split("\\|");
        String st = split[0];
        JSONObject jsonObject = new JSONObject(split[1]);
        String result = "";
        switch (key) {
            case "st":
                result = st;
                break;
            case "et":
                result = jsonObject.getString("et");
                break;
            default:  // 取公共字段
                JSONObject cm = jsonObject.getJSONObject("cm");
                if (cm.has(key)) {
                    result = cm.getString(key);
                }
                break;
        }

        return result;
    }

    public static void main(String[] args) throws JSONException {
        String line = "1573488007508|{\"cm\":{\"ln\":\"-96.0\",\"sv\":\"V2.0.9\",\"os\":\"8.0.2\",\"g\":\"92GXN992@gmail.com\",\"mid\":\"3\",\"nw\":\"WIFI\",\"l\":\"pt\",\"vc\":\"17\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"3\",\"t\":\"1573394999541\",\"la\":\"-44.1\",\"md\":\"HTC-9\",\"vn\":\"1.0.7\",\"ba\":\"HTC\",\"sr\":\"M\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1573422500912\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"2\",\"category\":\"19\"}},{\"ett\":\"1573482547727\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"0\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"433\",\"loading_way\":\"1\"}},{\"ett\":\"1573407322877\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"1\",\"action\":\"5\",\"detail\":\"\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"1\"}},{\"ett\":\"1573450105946\",\"en\":\"praise\",\"kv\":{\"target_id\":5,\"id\":7,\"type\":2,\"add_time\":\"1573426045796\",\"userid\":4}}]}";
        System.out.println(new BaseFieldUdf().evaluate(line, "et"));
        System.out.println(new BaseFieldUdf().evaluate(line, "st"));
        System.out.println(new BaseFieldUdf().evaluate(line, "os"));
        System.out.println(new BaseFieldUdf().evaluate(line, "mid"));
    }

}
