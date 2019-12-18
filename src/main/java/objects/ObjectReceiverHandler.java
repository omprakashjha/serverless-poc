package objects;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class ObjectReceiverHandler implements
        RequestHandler<S3Event, String> {

    private static final String QUEUE_NAME = "objects-queue";        

    public String handleRequest(S3Event s3event, Context context) {
        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");

            System.out.println("Reading file: " + srcBucket + "/" + srcKey);

            String dstBucket = "objects-output";
            String dstKey = "processed-" + srcKey;

            // Sanity check: validate that source and destination are different
            // buckets.
            if (srcBucket.equals(dstBucket)) {
                System.out
                        .println("Destination bucket must not match source bucket.");
                return "";
            }

            // Download the file from S3 into a stream
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                    srcBucket, srcKey));

            InputStream is = s3Object.getObjectContent();

            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentType("csv");

            //Send file name to SQS queue
            AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
            String queueUrl = sqsClient.getQueueUrl(QUEUE_NAME).getQueueUrl();

            System.out.println("Writing to sqs queue: " + queueUrl);
            SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(dstKey)
                .withDelaySeconds(5);
            sqsClient.sendMessage(send_msg_request);

            // Uploading to S3 destination bucket
            System.out.println("Writing to: " + dstBucket + "/" + dstKey);
            s3Client.putObject(dstBucket, dstKey, is, meta);

            return "Ok";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
