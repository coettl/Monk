package com.linemetrics.monk.store.plugins.mail;

import com.linemetrics.monk.processor.ProcessorException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Klemens on 01.03.2017.
 */
public class MailFacade {

    private static final Logger logger = LoggerFactory.getLogger(MailFacade.class);

    private static final String PROPERTY_SMTP_HOST = "mail.smtp.host";
    private static final String PROPERTY_SMTP_PORT = "mail.smtp.port";
    private static final String PROPERTY_SMTP_TLS = "mail.smtp.starttls.enable";
    private static final String PROPERTY_SMTP_AUTH = "mail.smtp.auth";
    private static final String PROPERTY_SMTP_SOCKET_PORT = "mail.smtp.socketFactory.port";
    private static final String PROPERTY_SMTP_SOCKET_CLASS = "mail.smtp.socketFactory.class";
    private static final String PROPERTY_SMTP_SOCKET_FALLBACK = "mail.smtp.socketFactory.fallback";

    private final Properties properties = System.getProperties();

    private final String sender;
    private final String user;
    private final String password;

    public MailFacade(final String sSmtpHost,
                       final String sSmtpPort,
                       final String sSmtpTls,
                       final String sSmtpUser,
                       final String sSmtpPassword,
                       final String sSender) throws ProcessorException {

        //validate
        if(sSmtpHost == null || sSmtpPort == null || sSmtpTls == null){
            throw new ProcessorException("Invalid mail params");
        }

        this.sender = sSender;
        this.user = sSmtpUser;
        this.password = sSmtpPassword;

        //set properties
        this.properties.put(PROPERTY_SMTP_HOST, sSmtpHost);
        this.properties.put(PROPERTY_SMTP_PORT, sSmtpPort);
        this.properties.put(PROPERTY_SMTP_TLS, sSmtpTls);
        this.properties.put(PROPERTY_SMTP_AUTH, "true");
        this.properties.put(PROPERTY_SMTP_SOCKET_PORT, sSmtpPort);
        this.properties.put(PROPERTY_SMTP_SOCKET_CLASS, "javax.net.ssl.SSLSocketFactory");
        this.properties.put(PROPERTY_SMTP_SOCKET_FALLBACK, "false");
    }

    /**
     *
     * @param sReceiver
     * @param sSubject
     * @param sText
     * @param lAttachmentList
     * @throws ProcessorException
     */
    public void send(final String sReceiver,
                     final String sSubject,
                     final String sText,
                     final List<BodyPart> lAttachmentList) throws ProcessorException {

        Session session = null;
        try {
            session = Session.getDefaultInstance(this.properties, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(user, password);
                }
            });
        } catch(Exception e){
            throw new ProcessorException("Error creating Mail-session: "+e.getMessage());
        }

        final MimeMessage message = new MimeMessage(session);
        final Multipart multipart = new MimeMultipart();
        final BodyPart bodypart = new MimeBodyPart();

        try {
            message.setFrom(new InternetAddress(this.sender));
            if(StringUtils.isNotEmpty(sReceiver)){
                for(final String receiver : Arrays.asList(sReceiver.split(","))){
                    message.addRecipient(Message.RecipientType.TO, new InternetAddress(receiver));
                }
            } else {
                throw new ProcessorException("Email Receiver is not defined");
            }
            message.setSubject(StringUtils.isNotEmpty(sSubject) ? sSubject : "Linemetrics CSV Export");
            bodypart.setText(StringUtils.isNotEmpty(sText) ? sText : "Der CSV-Export befindet sich im Anhang.");
            multipart.addBodyPart(bodypart);

            //add attachments
            if (lAttachmentList != null && lAttachmentList.size() > 0) {
                for (final BodyPart bp : lAttachmentList) {
                    if(bp != null){
                        multipart.addBodyPart(bp);
                    }
                }
            }

            // Send the complete message parts
            message.setContent(multipart);

            //finally send
            Transport.send(message);

        } catch (MessagingException e) {
            throw new ProcessorException("Error sending mail: " + e.getMessage());
        }
    }
}
