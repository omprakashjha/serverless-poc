package objects;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.AmazonSQSException;

public class ObjectProcessorHandler implements
        RequestHandler<SQSEvent, String> {

    public String handleRequest(SQSEvent sqsEvent, Context context) {
        try {
            String srcBucket = "objects-output";
            for(SQSMessage msg : sqsEvent.getRecords()){
                System.out.println("Message is: " + msg.getBody());

                // retrieve the file from S3 into a stream
                AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                        srcBucket, msg.getBody()));

                
                System.out.println("Message content is: " + getStringFromInputStream(s3Object.getObjectContent().getDelegateStream()));
            }

            return "Ok";
        } catch (AmazonSQSException e) {
            throw e;
        }
    }

    // convert InputStream to String
	private static String getStringFromInputStream(InputStream is) {

		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {

			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return sb.toString();

	}
}