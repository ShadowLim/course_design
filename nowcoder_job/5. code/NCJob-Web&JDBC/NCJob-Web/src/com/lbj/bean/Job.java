package com.lbj.bean;

/**
 * @description：
 * @date：Created in 19:17 2022/6/10
 */
public class Job {

    private static int id;
    private static String pub_date;
    private static String position;
    private static String address;
    private static String education;
    private static double salary;
    private static double rate;
    private static int duration;


    public static int getId() {
        return id;
    }

    public static void setId(int id) {
        Job.id = id;
    }

    public static String getPub_date() {
        return pub_date;
    }

    public static void setPub_date(String pub_date) {
        Job.pub_date = pub_date;
    }

    public static String getPosition() {
        return position;
    }

    public static void setPosition(String position) {
        Job.position = position;
    }

    public static String getAddress() {
        return address;
    }

    public static void setAddress(String address) {
        Job.address = address;
    }

    public static String getEducation() {
        return education;
    }

    public static void setEducation(String education) {
        Job.education = education;
    }

    public static double getSalary() {
        return salary;
    }

    public static void setSalary(double salary) {
        Job.salary = salary;
    }

    public static double getRate() {
        return rate;
    }

    public static void setRate(double rate) {
        Job.rate = rate;
    }

    public static int getDuration() {
        return duration;
    }

    public static void setDuration(int duration) {
        Job.duration = duration;
    }
}
