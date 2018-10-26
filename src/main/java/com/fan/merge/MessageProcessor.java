package com.fan.merge;

import com.fan.merge.dto.FileCombineDto;
import com.fan.merge.transform.CombineFile;
import com.fan.merge.transform.FileCombineFactory;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 从redis队列中读取消息，解析消息，构建合并小文件taks，提交到线程池中处理
 */
@Component("messageprocessor")
public class MessageProcessor {

    public static Logger logger = LogManager.getLogger(MessageProcessor.class);
    final int cpuCore = Runtime.getRuntime().availableProcessors();
    private Gson gson = new Gson();
    public static  final Configuration conf = new Configuration();

    @Autowired
    @Qualifier("redispools")
    JedisPoolz redispools;

    @Autowired
    @Qualifier("appconfig")
    private AppConfig appconfig;

    public MessageProcessor() {
    }

    public void run() {
        BlockingQueue<String> quenes = new LinkedBlockingDeque<String>(appconfig.getTaskQueneSize());
        new Thread(new Produce2Quene(quenes)).start();
        for (int i = 0; i < appconfig.getParallelism(); i++) {
            new Thread(new ConsumerFromQuene(quenes)).start();
        }
    }

    public void run_v2(){
        BlockingQueue<String> quenes = new LinkedBlockingDeque<String>(appconfig.getTaskQueneSize());
        new Thread(new Produce2Quene(quenes)).start();
        final int poolSize = (int)(cpuCore/(1-appconfig.getBloclageCoefficient()));
        logger.info("cpuCores["+cpuCore+"],poolSizes["+poolSize+"]");
        ExecutorService service = Executors.newFixedThreadPool(poolSize);
        try {
            while (true) {
                while (!quenes.isEmpty()) {
                    service.execute(new ConsumerFromQuene2(quenes));
                }
                Thread.sleep(1000);
            }
        } catch(InterruptedException e){
            e.printStackTrace();
        }finally {
            service.shutdown();
        }
    }


    class Produce2Quene implements Runnable {
        private BlockingQueue<String> quenes;

        public Produce2Quene(BlockingQueue<String> quenes) {
            this.quenes = quenes;
        }

        @Override
        public void run() {
            Jedis jedis = redispools.getResource();
            while (true) {
                logger.info(Thread.currentThread().getName()+" monitor redis .......");
                if (quenes.remainingCapacity() == 0) {
                    logger.info("quene is full ,thread wait");
                } else {
                    String message = jedis.rpop(appconfig.getQueneName());

                    if (message != null) {
                        logger.info("get message from redis and push to quene:"+message);
                        quenes.offer(message);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public void process(BlockingQueue<String> quenes){
        try {
            String message = quenes.poll();
            if (message != null) {
                logger.info(message);
                FileCombineDto fileCombineDto = gson.fromJson(message, FileCombineDto.class);
                CombineFile combine = FileCombineFactory.getCombine(fileCombineDto,appconfig);
                logger.info(appconfig);
                combine.combine(fileCombineDto,conf);
            }

        }catch (IOException e){
            e.printStackTrace();
        }
    }

    class ConsumerFromQuene implements Runnable {
        private BlockingQueue<String> quenes;

        public ConsumerFromQuene(BlockingQueue<String> quenes) {
            this.quenes = quenes;
        }

        @Override
        public void run() {
            while (true) {
                while (!quenes.isEmpty()) {
                    process(quenes);
                }
            }
        }
    }


    class ConsumerFromQuene2 implements Runnable {
        private BlockingQueue<String> quenes;

        public ConsumerFromQuene2(BlockingQueue<String> quenes) {
            this.quenes = quenes;
        }

        @Override
        public void run() {
            process(quenes);
        }
    }

}
