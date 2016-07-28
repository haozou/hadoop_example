package com.alpine.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;

import javax.net.ssl.SSLContext;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Created by Hao on 7/21/16.
 */
public class HTTPClinetExample extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("chorus/chorus.alpinenow.local@ALPINE", "/Users/Hao/workspace/hadoop_example/kerberos_example/chorus.keytab");
        ugi.doAs(new PrivilegedExceptionAction<HTTPClinetExample>() {
            public HTTPClinetExample run() throws Exception {
                HTTPClinetExample mr = new HTTPClinetExample();
                int res = ToolRunner.run(new Configuration(), mr, args);
                return mr;
            }
        });
    }

    @Override
    public int run(String[] args) throws Exception {
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy()
                {
                    public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException
                    {
                        return true;
                    }
                }).build();

        Credentials use_jaas_creds = new Credentials() {
            public String getPassword() {
                return null;
            }

            public Principal getUserPrincipal() {
                return null;
            }
        };
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(new AuthScope(null, -1, null), use_jaas_creds);
        Registry<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();

        CloseableHttpClient httpClient = HttpClients.custom().
                setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(30 * 1000).build()).
                setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).
                setDefaultAuthSchemeRegistry(authSchemeRegistry).
                setDefaultCredentialsProvider(credsProvider).
                setSSLContext(sslContext).build();
        HttpResponse response = httpClient.execute(new HttpGet("http://10.0.0.196:8088/ws/v1/cluster/metrics"));
        String result = org.apache.commons.io.IOUtils.toString(response.getEntity().getContent());
        System.out.println(result);
        return 0;
    }
}
