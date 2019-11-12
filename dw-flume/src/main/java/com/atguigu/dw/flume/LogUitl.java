package com.atguigu.dw.flume;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

/**
 * @Author lzc
 * @Date 2019/11/12 14:40
 */
public class LogUitl {
    public static boolean validateStart(String msg) {
        if (StringUtils.isBlank(msg.trim())) {
            return false;
        }

        if (!msg.trim().startsWith("{") || !msg.trim().endsWith("}")) {
            return false;
        }

        return true;
    }

    public static boolean validateEvent(String msg) {
        if (StringUtils.isBlank(msg.trim())) {
            return false;
        }

        String[] split = msg.split("\\|");  // 正则中 | 或   \|
        String ts = split[0].trim();
        if (ts.length() != 13 || !NumberUtils.isDigits(ts)) {
            return false;
        }

        String event = split[1].trim();
        if (!event.trim().startsWith("{") || !event.trim().endsWith("}")) {
            return false;
        }

        return true;
    }

    public static void main(String[] args) {
//        String msg = "{\"action\":\"1\",\"ar\":\"MX\",\"ba\":\"HTC\",\"detail\":\"\",\"en\":\"start\",\"entry\":\"2\",\"extend1\":\"\",\"g\":\"B449TS1U@gmail.com\",\"hw\":\"640*1136\",\"l\":\"pt\",\"la\":\"26.5\",\"ln\":\"-38.6\",\"loading_time\":\"17\",\"md\":\"HTC-14\",\"mid\":\"1\",\"nw\":\"4G\",\"open_ad_type\":\"2\",\"os\":\"8.2.3\",\"sr\":\"E\",\"sv\":\"V2.0.8\",\"t\":\"1573385434350\",\"uid\":\"1\",\"vc\":\"1\",\"vn\":\"1.0.9\"}";
//        System.out.println(validateStart(msg));
        String msg = "157346416977|{\"action\":\"1\",\"ar\":\"MX\",\"ba\":\"HTC\",\"detail\":\"\",\"en\":\"start\",\"entry\":\"2\",\"extend1\":\"\",\"g\":\"B449TS1U@gmail.com\",\"hw\":\"640*1136\",\"l\":\"pt\",\"la\":\"26.5\",\"ln\":\"-38.6\",\"loading_time\":\"17\",\"md\":\"HTC-14\",\"mid\":\"1\",\"nw\":\"4G\",\"open_ad_type\":\"2\",\"os\":\"8.2.3\",\"sr\":\"E\",\"sv\":\"V2.0.8\",\"t\":\"1573385434350\",\"uid\":\"1\",\"vc\":\"1\",\"vn\":\"1.0.9\"}";
        System.out.println(validateEvent(msg));
    }
}

