package com.fan.merge.dto;

import java.util.ArrayList;
import java.util.List;

public class FileCombinePersDto implements Comparable<FileCombinePersDto>{
    private List<FileStatusDto> list;
    private String fileName;
    private long fileSize;

    public List<FileStatusDto> getList() {
        return list;
    }

    public void setList(List<FileStatusDto> list) {
        this.list = list;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void add(FileStatusDto dto){
        if(list==null){
            list = new ArrayList<FileStatusDto>();
        }
        this.fileSize += dto.getFileSize();
        list.add(dto);
    }

    public FileCombinePersDto(String fileName) {
        this.fileName = fileName;
    }

    public void addFileSize(long fsz){
        this.fileSize += fsz;
    }

    @Override
    public int compareTo(FileCombinePersDto dist) {

        if(dist!=null){
            if(this.fileSize>=dist.fileSize){
                return 1;
            }else{
                return -1;
            }
        }

        return 0;
    }

    @Override
    public String toString() {
        return "FileCombinePersDto{" +
                "list=" + list +
                ", fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                '}';
    }
}
