package com.fan.merge.dto;

/**
 * 文件合并目录
 */
public class FileCombineDto {
    private String fileDir;
    private String type;
    private int fileSize;

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

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }


    @Override
    public String toString() {
        return "FileCombineDto{" +
                "fileDir='" + fileDir + '\'' +
                ", type='" + type + '\'' +
                ", fileSize=" + fileSize +
                '}';
    }
}
