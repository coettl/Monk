package com.linemetrics.monk.store.plugins.lm3;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RestFacade {

    private static final Logger logger = LoggerFactory.getLogger(RestFacade.class);
    private static final List<Integer> validResponseCodes = Arrays.asList(
        200, 201, 202, 301, 302
    );

//    private String        connectionUrl;

    private String        oauthClientId;
    private String        oauthClientSecret;
    private String        oauthClientUrl;

    private URL           oauthUrl;
    private URI           oauthUri;

    private CloseableHttpClient httpclient;

    private String 		token;
    private String 		tokenType;
    private Long 	    tokenExpiresIn;

    public RestFacade(String oauthClientId,
                      String oauthClientSecret,
                      String oauthClientUrl) throws Exception {

        this.oauthClientId 	   = oauthClientId;
        this.oauthClientId     = this.oauthClientId == null ? "" : this.oauthClientId;

        this.oauthClientSecret = oauthClientSecret;
        this.oauthClientSecret = this.oauthClientSecret == null ? "" : this.oauthClientSecret;

        this.oauthClientUrl    = oauthClientUrl;
        this.oauthClientUrl    = this.oauthClientUrl == null ? "" : this.oauthClientUrl;

        System.out.println("Initialize rest facade");

        this.httpclient = HttpClientBuilder.create().build();

        try {
            this.oauthUrl = new URL(this.oauthClientUrl);
            this.oauthUri = this.oauthUrl.toURI();

        } catch(MalformedURLException mue) {
            throw new Exception(mue.getMessage());
        }
    }

    public boolean send( URI uri,
                         String statement )
        throws IOException {



        HttpPost httppost = new HttpPost(uri);
        httppost.setHeader("Accept-Encoding", "UTF-8");
        httppost.setHeader("Content-type", "application/json");
        addAuthorization(httppost);

        StringEntity stringEntity = new StringEntity(statement);
        stringEntity.setContentType("application/json");
        stringEntity.setContentEncoding("UTF-8");
        httppost.setEntity(stringEntity);

        CloseableHttpResponse response = null;

        long time = System.currentTimeMillis();
        try {
            response = httpclient.execute(httppost);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                // EntityUtils to get the response content
                String content = EntityUtils.toString(entity);
                System.out.println(content);
            }

            System.out.println(response);

//            System.out.println("Do send: " + statement.replace("\n", "").replace("\r", "") + " to " + uri + " with token key " + this.token + " -> received: " + response.getStatusLine());
            System.out.format("Sending %.2f kb of data took %.2f seconds!", ((0.0 + statement.length()) / 1024), ((0.0 + System.currentTimeMillis()) - time) / 1000);
        } catch(Exception exp) {
//            System.out.println("Sending data failed: " + statement.replace("\n", "").replace("\r", "") + " to " + uri + " with token key " + this.token);
            System.err.println("Sending of data failed!");
            httpclient.close();
            httpclient = HttpClientBuilder.create().build();
        } finally {

            if(response != null) {
                HttpClientUtils.closeQuietly(response);
            }
        }

        System.out.format("Sending %.2f kb of data took %.2f seconds!", ((0.0 + statement.length()) / 1024), ((0.0 + System.currentTimeMillis()) - time) / 1000);

        return response != null && validResponseCodes.contains(response.getStatusLine().getStatusCode());
    }

    protected void addAuthorization(HttpPost request) throws IOException {

        if( token == null ||
            tokenExpiresIn < getCurrentTimeSeconds()) {
            System.out.println("Renew oauth token!");
            getKey();
        }

        if(token == null || tokenType == null) {
            throw new IOException("Unable to receive oauth token!");
        }

        request.addHeader("Authorization", tokenType + " " +token);
    }

    protected static long getCurrentTimeSeconds() {
        return Math.round(System.currentTimeMillis() / 1000);
    }

    protected void getKey() {
        HttpPost httppost = new HttpPost(oauthUri);

        //ParamList
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("client_id",  oauthClientId ));
        params.add(new BasicNameValuePair("grant_type", "client_credentials"));
        params.add(new BasicNameValuePair("client_secret", oauthClientSecret));

        HttpResponse response = null;

        try {
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

            response = httpclient.execute(httppost);
            HttpEntity entity = response.getEntity();

            token = tokenType = null;
            tokenExpiresIn = null;

            if (entity != null) {
                // EntityUtils to get the response content
                String content =  EntityUtils.toString(entity);
                final JSONObject obj = (JSONObject) new JSONParser().parse(content);
                token = (String) obj.get("access_token");
                tokenType = (String) obj.get("token_type");

                tokenExpiresIn = (Long) obj.get("expires_in");
                tokenExpiresIn += Math.round(System.currentTimeMillis() / 1000) - 60;
            }
        } catch (ParseException | IOException e) {
            e.printStackTrace();
        } finally {
            if(response != null) {
                HttpClientUtils.closeQuietly(response);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println(Boolean.valueOf("true"));

        StringBuffer sb = new StringBuffer();
        sb	.append("{")
            .append("'url' : 'http://localhost:4567/public/rest-data', ")
            .append("'oauth_activate' : 1, ")
            .append("'oauth_client_id' : 'lm2', ")
            .append("'oauth_client_secret' : '1234', ")
            .append("'oauth_client_url' : 'http://localhost:4567/public/rest-auth'")
            .append("}");

//        RestFacade rest = new RestFacade(config);
//
//
//        for(int i = 0; i < 2048; i++) {
//            boolean success = rest.send(
//                12345L,
//                (Map) ImmutableMap.of(
//                    LDFEDataItemColumns.TIMESTAMP, System.currentTimeMillis(),
//                    LDFEDataItemColumns.VALUE, "11"),
//                BatchTypeFinder.TimedDataBatch.Minute);
//        }

        System.out.println("Ready");

    }

}

