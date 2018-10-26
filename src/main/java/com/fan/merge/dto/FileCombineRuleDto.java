package com.fan.merge.dto;


/**
 * 文件合并dto
 */
public class FileCombineRuleDto {

    private int fileSize;//默认单位mb
    private String fileDir;//文件路径
    private String type;//文件类型：parquet，orc
    private int startTime;//开始时间20180101
    private int endTime;//结束时间20181001

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public String getFileDir() {
        return fileDir;
    }

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getStartTime() {
        return startTime;
    }

    public void setStartTime(int startTime) {
        this.startTime = startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public void setEndTime(int endTime) {
        this.endTime = endTime;
    }


    @Override
    public String toString() {
        return "FileCombineRuleDto{" +
                "fileSize=" + fileSize +
                ", fileDir='" + fileDir + '\'' +
                ", type='" + type + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
