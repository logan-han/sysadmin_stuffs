import com.santaba.agent.groovyapi.http.*;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateFactory;

hostname = "blah.okta.com";
app_id = "blah";

url = "https://" + hostname + "/app/" + app_id + "/sso/saml/metadata";

response_text = HTTP.body(url);

response = new XmlSlurper().parseText(response_text);
x509_text = response.'**'.find{it.name() == 'X509Certificate'}.text();
CertificateFactory cf = CertificateFactory.getInstance("X.509");
byte [] decoded = x509_text.decodeBase64();
InputStream is = new ByteArrayInputStream(decoded);
X509Certificate x509_cert = cf.generateCertificate(is);
Date expiry = x509_cert.getNotAfter();
Date today = new Date();

use(groovy.time.TimeCategory) {
   def duration = expiry - today
   println "remaning_days: ${duration.days}"
}
return (0);
