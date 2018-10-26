package com.fan.merge;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component("appconfig")
public class AppConfig {

    @Value("${filesize:128m}")
    private String filesize;

    @Value("${start.time:20180101}")
    private int startTime;

    @Value("${end.time:30}")
    private int endTime;

    @Value("${file.dir.key:FILEDIRKEY}")
    private String fileDirKey;

    @Value("${quene.name:FILECOMBINEQUENE}")
    private String queneName;

    @Value("${default.parallelism:3}")
    private int parallelism;

    @Value("${task.quene.size:10}")
    private int taskQueneSize;

    @Value("${tmp.dir:/user/combine}")
    private String tmpDir;

    @Value("${blockage.coefficient:0.8}")
    private double bloclageCoefficient;

    @Autowired
    private Environment environment;

    public String getFilesize() {
        return filesize;
    }

    public void setFilesize(String filesize) {
        this.filesize = filesize;
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

    public Environment getEnvironment() {
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }


    public String getFileDirKey() {
        return fileDirKey;
    }

    public void setFileDirKey(String fileDirKey) {
        this.fileDirKey = fileDirKey;
    }

    public String getQueneName() {
        return queneName;
    }

    public void setQueneName(String queneName) {
        this.queneName = queneName;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getTaskQueneSize() {
        return taskQueneSize;
    }

    public void setTaskQueneSize(int taskQueneSize) {
        this.taskQueneSize = taskQueneSize;
    }

    public String getTmpDir() {
        return tmpDir;
    }

    public void setTmpDir(String tmpDir) {
        this.tmpDir = tmpDir;
    }

    public double getBloclageCoefficient() {
        return bloclageCoefficient;
    }

    public void setBloclageCoefficient(double bloclageCoefficient) {
        this.bloclageCoefficient = bloclageCoefficient;
    }


    @Override
    public String toString() {
        return "AppConfig{" +
                "filesize='" + filesize + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", fileDirKey='" + fileDirKey + '\'' +
                ", queneName='" + queneName + '\'' +
                ", parallelism=" + parallelism +
                ", taskQueneSize=" + taskQueneSize +
                ", tmpDir='" + tmpDir + '\'' +
                ", bloclageCoefficient=" + bloclageCoefficient +
                ", environment=" + environment +
                '}';
    }
}
