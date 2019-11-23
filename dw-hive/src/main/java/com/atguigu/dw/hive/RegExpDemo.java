package com.atguigu.dw.hive;

/**
 * @Author lzc
 * @Date 2019/11/23 15:57
 */
public class RegExpDemo {

    public static void main(String[] args) {
        String s = ".";

        System.out.println(s.matches("\\."));

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

[abc] a或者b或者c
[a-z] 小写字母
[a-zA-Z0-9_] 数字字母下划线
[^a] 非a  ^在[]内才表示非
\w   w:word 单词字符串  ==== [a-zA-Z0-9_]
\W   非单词字符:
\d   d:digital  数字 ==== [0-9]
\D   非数字
\s   s:space 空白字符串  \t \r \n
\S   非空白字符
. 任意字符  (除了: \n, \r)
\. 匹配点


 */