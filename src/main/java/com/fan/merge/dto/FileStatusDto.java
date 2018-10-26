package com.fan.merge.dto;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileStatusDto implements Comparable<FileStatusDto> {

    private Path path;
    private long fileSize;
    private long modification_time;

    @Override
    public int compareTo(FileStatusDto dist) {
        if (dist != null) {
            if (this.fileSize >= dist.fileSize) {
                return -1;
            } else {
                return 1;
            }
        } else {
            return 1;
        }
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getModification_time() {
        return modification_time;
    }

    public void setModification_time(long modification_time) {
        this.modification_time = modification_time;
    }


    public FileStatusDto() {
    }

    public FileStatusDto(Path path, long fileSize, long modification_time) {
        this.path = path;
        this.fileSize = fileSize;
        this.modification_time = modification_time;
    }

    @Override
    public String toString() {
        return "FileStatusDto{" +
                "path='" + path + '\'' +
                ", fileSize=" + fileSize +
                ", modification_time=" + modification_time +
                '}';
    }

}
