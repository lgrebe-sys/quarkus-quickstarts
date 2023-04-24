package org.acme.s3;

import org.jboss.logging.Logger;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import java.util.Arrays;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import org.jboss.resteasy.reactive.MultipartForm;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;

@Path("/s3")
public class S3SyncClientResource extends CommonResource {
    @Inject
    S3Client s3;

    private static final Logger LOG = Logger.getLogger(S3SyncClientResource.class);

    String bucket = "quarkus.s3.quickstart";
    String filename = "testfile";

    @POST
    @Path("upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadFile(@MultipartForm FormData formData) throws Exception {

        if (formData.filename == null || formData.filename.isEmpty()) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        if (formData.mimetype == null || formData.mimetype.isEmpty()) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        if (formData.data.length() > 5242880) {
            LOG.info("File is bigger than 5 MB");
            final CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(filename)
                .build();
            CreateMultipartUploadResponse createMultipartUploadResponse = s3.createMultipartUpload(createMultipartUploadRequest);
            int partNumber = 1;
            int uploadedBytes = 0;
            List <CompletedPart> completedPartList = new ArrayList <> ();
            while (uploadedBytes < formData.data.length()) {
                int endIndex = uploadedBytes + 5242880;
                completedPartList.add(uploadPart(filename, FileUtils.readFileToByteArray(formData.data), createMultipartUploadResponse, partNumber, uploadedBytes, endIndex));
                partNumber++;
                uploadedBytes = endIndex;
            }
            CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder().parts(completedPartList).build();
            final CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(filename)
                .uploadId(createMultipartUploadResponse.uploadId())
                .multipartUpload(multipartUpload)
                .build();
            s3.completeMultipartUpload(completeMultipartUploadRequest);
            LOG.info("Finished upload");
            return Response.ok().status(Status.CREATED).build();
        } else {
            LOG.info("Attempt to write file " + filename);
            final PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(filename)
                .build();
            s3.putObject(putRequest, RequestBody.fromBytes(FileUtils.readFileToByteArray(formData.data)));
            return Response.ok().status(Status.CREATED).build();
        }
    }


    public CompletedPart uploadPart(String fileName, byte[] fileContent, CreateMultipartUploadResponse createMultipartUploadResponse, int partNumber,
        int uploadedBytes, int endIndex) {
        LOG.info("Uploading Part " + partNumber + " of file " + filename);
        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
            .bucket(bucket)
            .key(fileName)
            .uploadId(createMultipartUploadResponse.uploadId())
            .partNumber(partNumber)
            .build();
        UploadPartResponse uploadPartResponse = s3.uploadPart(uploadPartRequest, RequestBody.fromBytes(
            Arrays.copyOfRange(fileContent, uploadedBytes, Math.min(endIndex, fileContent.length))));
        return CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build();
    }


    @GET
    @Path("download/{objectKey}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response downloadFile(String objectKey) {
        ResponseBytes <GetObjectResponse> objectBytes = s3.getObjectAsBytes(buildGetRequest(objectKey));
        ResponseBuilder response = Response.ok(objectBytes.asUtf8String());
        response.header("Content-Disposition", "attachment;filename=" + objectKey);
        response.header("Content-Type", objectBytes.response().contentType());
        return response.build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List <FileObject> listFiles() {
        ListObjectsRequest listRequest = ListObjectsRequest.builder().bucket(bucketName).build();

        //HEAD S3 objects to get metadata
        return s3.listObjects(listRequest).contents().stream()
                .map(FileObject::from)
                .sorted(Comparator.comparing(FileObject::getObjectKey))
                .collect(Collectors.toList());
    }
}
