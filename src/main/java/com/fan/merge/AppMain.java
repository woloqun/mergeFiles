package com.fan.merge;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;


/**
 * 文件合并入口
 */
public class AppMain {

    public static void main(String[] args) {
        AbstractApplicationContext context = new AnnotationConfigApplicationContext(ConfigParser.class);
        MessageProcessor messageprocessor = (MessageProcessor) context.getBean("messageprocessor");
        messageprocessor.run_v2();
//        messageprocessor.run();

    }
}
