package com.ippon.kafka.basic;

/**
 * Created by @ImFlog on 15/02/2017.
 */
public class BasicProduceApp {

    public static void main(String[] args) throws InterruptedException {
        // Set producer input file from command line args
        BasicProducer.INPUT_FILE = args[0];

        new Thread(new BasicProducer()).start();
    }
}