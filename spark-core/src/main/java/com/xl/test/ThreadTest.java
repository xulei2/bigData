package com.xl.test;

public class ThreadTest {

   private static ThreadLocal local= new ThreadLocal<String>();

   public static void put(String str){
       local.set(str);
   }
   public static String get(){
       return local.get().toString();
   }
}
