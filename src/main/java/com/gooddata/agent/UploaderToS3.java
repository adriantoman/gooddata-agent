/*
 * Copyright (c) 2014, GoodData Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice, this list of conditions and
 *        the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 *        and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *     * Neither the name of the GoodData Corporation nor the names of its contributors may be used to endorse
 *        or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.gooddata.agent;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public class UploaderToS3 implements UploaderInterface {
	private final AmazonS3 s3client;
	private final String baseUrl;
	private final String bucket;

	public UploaderToS3(final String bucket, final String baseUrl, final String access_key, final String secret_key) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(access_key,secret_key);
        this.s3client = AmazonS3ClientBuilder.standard().withRegion("us-east-1").withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();

		final String slash = baseUrl.endsWith("/") ? "" : "/";
        this.baseUrl = baseUrl + slash;
        this.bucket = bucket;
    }

	public void upload(final Map<File,String> filesToUpload, final String remoteDir) throws IOException {
		for (Map.Entry<File, String> e : filesToUpload.entrySet()) {
			try {
				String keyName = baseUrl + e.getValue();
				System.out.println("Uploading file:");
				System.out.println(e.getKey() + "->" + keyName);

				s3client.putObject(new PutObjectRequest(bucket, keyName, e.getKey()));
			} catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which " +
						"means your request made it " +
						"to Amazon S3, but was rejected with an error response" +
						" for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
				throw new IOException("Upload to S3 failed");
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which " +
						"means the client encountered " +
						"an internal error while trying to " +
						"communicate with S3, " +
						"such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
				throw new IOException("Upload to S3 failed");
			}
		}
	}

//	public String uploadTemp(final File fileToUpload, final String remoteDir, final String remoteFileName) throws IOException {
//		final String tempUrl = toTempPath(baseUrl + remoteFileName);
//	    PutMethod method = new PutMethod(tempUrl);
//	    RequestEntity requestEntity = new InputStreamRequestEntity(new FileInputStream(fileToUpload));
//	    method.setRequestEntity(requestEntity);
//	    client.getParams().setAuthenticationPreemptive(true);
//	    client.executeMethod(method);
//	    if (method.getStatusCode() != HttpStatus.SC_CREATED) {
//	    	throw new RuntimeException(format("Upload failed: %s (status code = %d)",
//	    			method.getStatusText(), method.getStatusCode()));
//	    }
//	    return tempUrl;
//	}
//
//	private void move(final String remoteDir, final String tempUrl, String targetName) throws HttpException, IOException {
//		final String targetUrl = baseUrl + targetName;
//		System.out.println(tempUrl + " -> " + targetUrl);
//		MoveMethod method = new MoveMethod(tempUrl, targetUrl, true);
//		client.getParams().setAuthenticationPreemptive(true);
//		client.executeMethod(method);
//		final int sc = method.getStatusCode();
//	    if (sc != HttpStatus.SC_CREATED && sc != HttpStatus.SC_NO_CONTENT) {
//	    	throw new RuntimeException(format("Move failed: %s (status code = %d); file uploaded to " + tempUrl,
//	    			method.getStatusText(), method.getStatusCode()));
//	    }
//	}
//
//	private String toTempPath(final String path) {
//		return path + "." + System.currentTimeMillis();
//	}
}
