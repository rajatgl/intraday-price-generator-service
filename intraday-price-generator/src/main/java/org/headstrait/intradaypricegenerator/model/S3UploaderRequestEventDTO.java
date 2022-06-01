package org.headstrait.intradaypricegenerator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class S3UploaderRequestEventDTO {
    private String fileName;
    private UUID requestId;
    private List<String> headers;
    private String data;
    private int numberOfRows;

    //format: dd-Month-yyyy/STOCK
    private String s3FilePath;

    //name of the s3 bucket to be uploaded onto.
    private String bucketName;
}
