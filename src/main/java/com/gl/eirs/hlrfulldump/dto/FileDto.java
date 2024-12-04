package com.gl.eirs.hlrfulldump.dto;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Data
@Service
public class FileDto {

    String fileName;
    String filePath;

    long totalRecords;

    long successRecords;
    long failedRecords;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public FileDto(String fileName, String filePath, long totalRecords, long successRecords, long failedRecords) {
        this.fileName = fileName;
        this.filePath = filePath;
        this.totalRecords = totalRecords;
        this.successRecords = successRecords;
        this.failedRecords = failedRecords;
    }

    public FileDto() {
    }

    public FileDto(String fileName, String filePath) {
        this.fileName = fileName;
        this.filePath = filePath;
        this.totalRecords = getFileRecordCount(filePath + "/" + fileName);
        this.successRecords = 0;
        this.failedRecords = 0;
    }


    public long getFileRecordCount(String file) {
        try {
            File file1 = new File(file);
            logger.info("Getting the file size for file {}", file1.toURI());
            Path pathFile = Paths.get(file1.toURI());
            return (long) Files.lines(pathFile).count();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0L;
    }


}
