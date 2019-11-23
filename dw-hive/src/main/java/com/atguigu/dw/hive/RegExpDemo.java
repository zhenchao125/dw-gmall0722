package com.atguigu.dw.hive;

import java.util.Arrays;

/**
 * @Author lzc
 * @Date 2019/11/23 15:57
 */
public class RegExpDemo {

    public static void main(String[] args) {
//        String s = "1111abc@sohu.com.cn";

//        System.out.println(s.matches("1[2-9]\\d{9}"));
//        System.out.println(s.matches("\\w{3,15}@\\S+\\.(com|cn|org|edu|com\\.cn)"));

//        String s= "abd12wfjlej4u93298ufjsldjf4uu39";
//        System.out.println(s.replaceAll("\\d+", "z"));

//        String s = "我我我要要请你们们去去洗洗洗脚";
//        System.out.println(s.replaceAll("(.)\\1+", "$1"));

        String s = "..10.1.1......1..";
        String[] arr = s.split("\\.");
        System.out.println(Arrays.toString(arr));


    }
}

/*
正则表达式 RegularExpression
是一个工具, 用来处理文本: 匹配, 替换, 切割, 查找过滤

法律          正则

贪官          字符串

java中有两个类用来支持正则操作:
    Pattern  模式
    Matcher  匹配器

java的字符串有4个方法, 是支持直接写正则,
    match
    split
    replaceAll
    replaceFirst

[abc]         a或者b或者c
[a-z]         小写字母
[a-zA-Z0-9_]  数字字母下划线
[^a]          非a  ^在[]内才表示非
\w            w:word 单词字符串  ==== [a-zA-Z0-9_]
\W            非单词字符:
\d            d:digital  数字 ==== [0-9]
\D            非数字
\s            s:space 空白字符串  \t \r \n
\S            非空白字符
.             任意字符  (除了: \n, \r)
\.            匹配点

数量词:
a{m}  正好m个a
a{m,}  至少m个
a{m,n}  至少m个, 最多n个
a+ 至少1个a  === {1,}\
a* 至少0个a


\n 在正则表达式中表示取第n组   $n在替换的位置取第n组
 */