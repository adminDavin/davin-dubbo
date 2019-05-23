package com.davin.connector;



import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.util.StopWatch;

import com.davin.connector.hbase.HbaseConnector;


@Configuration
@PropertySource("classpath:/application.properties")
@ComponentScans({@ComponentScan("com.davin.connector.hbase")})
public class Boot {
    public static ApplicationContext ctx;
    public static void main(String[] args) {
        ctx = new AnnotationConfigApplicationContext(AppConfig.class, Boot.class);
        Boot boot = ctx.getBean(Boot.class);
        boot.hbaseConnectorMainFlow();
    }
    
    public void hbaseConnectorMainFlow() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 200, TimeUnit.MILLISECONDS,  new ArrayBlockingQueue<Runnable>(5));
        for(int i=0;i<3;i++){
            HbaseConnectorMainFlow myTask = new HbaseConnectorMainFlow(i);
            executor.execute(myTask);
            System.out.println("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+
            executor.getQueue().size()+"，已执行玩别的任务数目："+executor.getCompletedTaskCount());
        }
        executor.shutdown();
        
    }
    
    public static class HbaseConnectorMainFlow implements Runnable {
        private int taskNum;
        public HbaseConnectorMainFlow(int num) {
            this.taskNum = num;
        }
        
        @Override
        public void run() {
            System.out.println("正在执行task "+taskNum); 
            HbaseConnector connector = ctx.getBean(HbaseConnector.class);
            try {
                String tableName = "key_words";
                String[] columnNames = {"document", "key_word"};
                connector.createTable(tableName, columnNames);
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
//                connector.readData(tableName, columnNames, 10000*(taskNum), 1000000*(taskNum));
                connector.selectOneRow(tableName, "documentname99997", columnNames);
                connector.scanTable(tableName);
                stopWatch.stop();
                System.out.println("take time:"+ taskNum +stopWatch.getTotalTimeMillis());
            } catch (Throwable e) {
                e.printStackTrace();
            }
            System.out.println("task "+taskNum+"执行完毕");
        }
        
    }
}
