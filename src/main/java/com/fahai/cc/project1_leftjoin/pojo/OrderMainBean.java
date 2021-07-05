package com.fahai.cc.project1_leftjoin.pojo;

import java.util.Date;

public class OrderMainBean {
    private Long oid;
    private Date create_time;
    private Double total_money;
    private int status;
    private Date update_time;
    private String province;
    private String uid;
    //对数据库的操作类型：INSERT、UPDATE
    private String type;

    public OrderMainBean() {
    }

    public OrderMainBean(Long oid, Date create_time, Double total_money, int status, Date update_time,
                         String province, String uid, String type) {
        this.oid = oid;
        this.create_time = create_time;
        this.total_money = total_money;
        this.status = status;
        this.update_time = update_time;
        this.province = province;
        this.uid = uid;
        this.type = type;
    }

    @Override
    public String toString() {
        return "OrderMainBean{" +
                "oid=" + oid +
                ", create_time=" + create_time +
                ", total_money=" + total_money +
                ", status=" + status +
                ", update_time=" + update_time +
                ", province='" + province + '\'' +
                ", uid='" + uid + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
    public Long getOid() {
        return oid;
    }
    public void setOid(Long oid) {
        this.oid = oid;
    }
    public Date getCreate_time() {
        return create_time;
    }
    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }
    public Double getTotal_money() {
        return total_money;
    }
    public void setTotal_money(Double total_money) {
        this.total_money = total_money;
    }
    public int getStatus() {
        return status;
    }
    public void setStatus(int status) {
        this.status = status;
    }
    public Date getUpdate_time() {
        return update_time;
    }
    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }
    public String getProvince() {
        return province;
    }
    public void setProvince(String province) {
        this.province = province;
    }
    public String getUid() {
        return uid;
    }
    public void setUid(String uid) {
        this.uid = uid;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
}
