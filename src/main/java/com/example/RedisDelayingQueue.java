package com.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.UUID;

public class RedisDelayingQueue<T> {
    //内部静态类
    static class TaskItem<T>{
        public String id;
        public T msg;
    }

    private Type taskType=new TypeReference<TaskItem<T>>(){}.getType();

    private Jedis jedis;
    private String queueKey;

    public RedisDelayingQueue(Jedis jedis, String queueKey) {
        this.jedis = jedis;
        this.queueKey = queueKey;
    }

    public void delay(T msg){
        TaskItem<T> task=new TaskItem();
        task.id= UUID.randomUUID().toString();
        task.msg=msg;
        String s= JSON.toJSONString(task);
        jedis.zadd(queueKey,System.currentTimeMillis()+5000,s);
        System.out.println("放入队列:"+msg);
    }

    public void loop(){
        while (!Thread.interrupted()){
            Set<String> values= jedis.zrangeByScore(queueKey,0,System.currentTimeMillis(),0,1);
            if(values.isEmpty()){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
                continue;
            }
            String s=values.iterator().next();
            if(jedis.zrem(queueKey,s)>0){
                TaskItem<T> task= JSON.parseObject(s,taskType);
                this.handMessage(task.msg);
            }
        }
    }

    private void handMessage(T msg) {
        System.out.println("消费消息:"+msg);
    }

    public static void main(String[] args) {

        Jedis jedis=new Jedis("192.168.230.101",6379);
        RedisDelayingQueue redisDelayingQueue=new RedisDelayingQueue(jedis,"q-demo");
        Thread producer=new Thread(){
            @Override
            public void run() {
                for (int i=0;i<10;i++){
                    redisDelayingQueue.delay("codehole"+i);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        Thread consumer=new Thread(){
            @Override
            public void run() {
                redisDelayingQueue.loop();
            }
        };
        try {
        producer.start();
        //producer.start();只能调用一次start()方法

        producer.join();

        consumer.start();
        System.out.println("开始睡眠6秒");
        //Thread.sleep(6000);
        //consumer.interrupt();
        //consumer.join();
        System.out.println("执行完毕");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
