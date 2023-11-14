package cn.itcast.hotel.controller;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@RestController
@RequestMapping("/upload")
@Slf4j
public class UploadController {
    static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("/yyy/MM/dd/");

    // 资源的 访问 URL
    @Value("${app.minio.base-url}")
    private String baseUrl;

    // API 端点
    @Value("${app.minio.endpoint}")
    private String endpoint;

    // Bucket 存储桶
    @Value("${app.minio.bucket}")
    private String bucket;

    // Access Key
    @Value("${app.minio.access-key}")
    private String accessKey;

    // Secret Key
    @Value("${app.minio.secret-key}")
    private String secretKey;


    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> upload(@RequestParam("file")MultipartFile file) throws Exception{

        // 文件大小
        long size = file.getSize();
        if(size == 0){
            return ResponseEntity.badRequest().body("禁止上传空文件");
        }

        // 文件名称
        String fileName = file.getOriginalFilename();
        // 文件后缀
        String ext = "";
        int index = fileName.lastIndexOf(".");
        if(index == -1){
            return ResponseEntity.badRequest().body("禁止上传无后缀的文件");
        }

        ext = fileName.substring(index);

        // 文件类型
        String contentType = file.getContentType();
        if(contentType == null){
            contentType = MediaType.APPLICATION_OCTET_STREAM_VALUE;
        }

        // 根据日期打散目录
        String filePath = formatter.format(LocalDateTime.now()) + UUID.randomUUID().toString().replace("-", "") + ext;

        log.info("文件名称: {}", fileName);
        log.info("文件大小: {}", size);
        log.info("文件类型: {}", contentType);
        log.info("文件路径: {}", filePath);

        MinioClient client = MinioClient.builder()
                .endpoint(this.endpoint)
                .credentials(this.accessKey, this.secretKey)
                .build();
        try(InputStream inputStream = file.getInputStream()){
            client.putObject(PutObjectArgs.builder()
                    .bucket(this.bucket)
                    .contentType(contentType)
                    .object(filePath)
                    .stream(inputStream, size, -1)
                    .build());
        }
        return ResponseEntity.ok(this.baseUrl + this.bucket + filePath);
    }

}


