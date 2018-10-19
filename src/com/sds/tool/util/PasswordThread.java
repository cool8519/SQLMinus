package com.sds.tool.util;

public class PasswordThread implements Runnable {

    private boolean flag;


    public PasswordThread(String prompt) {
        System.out.print(prompt);
    }


    public void run() {
        flag = true;
        while(flag) {
            System.out.print("\010 ");
            try {
				Thread.sleep(1);
            } catch(InterruptedException ie) {
                System.out.println(ie.getMessage().trim());
            }
        }
    }


    public void stopMasking() {
        this.flag = false;
    }

}