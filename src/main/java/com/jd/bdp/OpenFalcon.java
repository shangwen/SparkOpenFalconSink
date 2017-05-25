package com.jd.bdp;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;


/**
 * Created by tangshangwen on 17-3-6.
 */
public class OpenFalcon {

    public static final int DEFAULT_BATCH_SIZE_LIMIT = 10;
    public static final int CONN_TIMEOUT_DEFAULT_MS = 5000;
    public static final int READ_TIMEOUT_DEFAULT_MS = 5000;
    public static final Logger logger = LoggerFactory.getLogger(OpenFalcon.class);

    public static Builder forService(String baseUrl) {
        return new Builder(baseUrl);
    }

    private final AsyncHttpClient.BoundRequestBuilder requestBuilder;
    private ObjectMapper mapper = new ObjectMapper();
    private int batchSizeLimit = DEFAULT_BATCH_SIZE_LIMIT;
    private AsyncHttpClient ahc;

    public static class Builder {
        private Integer connectionTimeout = CONN_TIMEOUT_DEFAULT_MS;
        private Integer readTimeout = READ_TIMEOUT_DEFAULT_MS;
        private String baseUrl;

        public Builder(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public Builder withConnectTimeout(Integer connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder withReadTimeout(Integer readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public OpenFalcon create() {
            return new OpenFalcon(baseUrl, connectionTimeout, readTimeout);
        }
    }

    private OpenFalcon(String baseURL, Integer connectionTimeout, Integer readTimeout) {
        AsyncHttpClientConfig acc = new AsyncHttpClientConfig.Builder()
                .setConnectTimeout(connectionTimeout)
                .setReadTimeout(readTimeout)
                .build();
        ahc = new AsyncHttpClient(acc);
        this.requestBuilder = ahc.preparePatch(baseURL + "/v1/push");
    }

    public void setBatchSizeLimit(int batchSizeLimit) {
        this.batchSizeLimit = batchSizeLimit;
    }

    public void send(Set<OpenFalconMetric> metrics) {
        sendHelper(metrics);
    }

    private void sendHelper(Set<OpenFalconMetric> metrics) {
        if (!metrics.isEmpty()) {
            try {
                requestBuilder
                        .setBody(mapper.writeValueAsString(metrics))
                        .execute(new AsyncCompletionHandler<Void>() {
                            public Void onCompleted(Response response) throws Exception {
                                if (response.getStatusCode() != 200) {
                                    logger.error("send to open falcon endpoint failed: ("
                                            + response.getStatusCode() + ") "
                                            + response.getResponseBody());
                                }
                                return null;
                            }
                        });
            } catch (Throwable ex) {
                logger.error("send to open falcon endpoint failed", ex);
            }
        }
    }

    public void close() {
        logger.debug("ahc isClosed {}", ahc.isClosed());
        if (!ahc.isClosed()) {
            ahc.close();
        }
    }
}
