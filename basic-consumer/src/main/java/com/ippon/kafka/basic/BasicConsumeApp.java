package com.ippon.kafka.basic;

/**
 * Created by @ImFlog on 15/02/2017.
 */
public class BasicConsumeApp {

    public static void main(String[] args) throws InterruptedException {

        new Thread(new BasicConsumer()).start();
    }
}
