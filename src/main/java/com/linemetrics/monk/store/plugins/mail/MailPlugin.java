package com.linemetrics.monk.store.plugins.mail;

import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.helper.TemplateParser;
import com.linemetrics.monk.processor.ProcessorException;
import com.linemetrics.monk.store.IStore;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.DataHandler;
import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.util.ByteArrayDataSource;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Klemens on 01.03.2017.
 */
public class MailPlugin implements IStore {

    private static final Logger logger = LoggerFactory.getLogger(MailPlugin.class);


    @Override
    public boolean store(RunnerContext ctx, JSONObject settings, Map<String, String> metaInfos, Map<Integer, Map<String, String>> dataStreamMetaInfos, Map<Integer, List<DataItem>> items) throws ProcessorException {

        final String numberLocale =
                settings.containsKey("csv_number_locale")
                        ? (String) settings.get("csv_number_locale") : "de_AT";

        final String headerTemplate =
                settings.containsKey("csv_header_template")
                        ? (String) settings.get("csv_header_template") : null;

        final String lineTemplate =
                settings.containsKey("csv_line_template")
                        ? (String) settings.get("csv_line_template") : null;

        final String fileTemplate =
                settings.containsKey("csv_file_template")
                        ? (String) settings.get("csv_file_template") : null;

        String lineSeparator =
                settings.containsKey("csv_line_separator")
                        ? (String) settings.get("csv_line_separator") : System.lineSeparator();
        lineSeparator = lineSeparator.replaceAll("<CR>", "\r").replaceAll("<LF>", "\n");

        final String mailReceiver =
                settings.containsKey("mail_receiver")
                        ? (String) settings.get("mail_receiver") : null;

        final String mailSubject =
                settings.containsKey("mail_subject")
                        ? (String) settings.get("mail_subject") : null;

        final Boolean mailAttachment =
                settings.containsKey("mail_attachment")
                        ? Boolean.valueOf((String) settings.get("mail_attachment")) : false;

        final String mailSmtpSender = settings.containsKey("mail_smtp_sender")
                ? (String) settings.get("mail_smtp_sender") : null;

        final String mailSmtpHost = settings.containsKey("mail_smtp_host")
                ? (String) settings.get("mail_smtp_host") : null;

        final String mailSmtpUser = settings.containsKey("mail_smtp_user")
                ? (String) settings.get("mail_smtp_user") : null;

        final String mailSmtpPw = settings.containsKey("mail_smtp_password")
                ? (String) settings.get("mail_smtp_password") : null;

        final String mailSmtpPort = settings.containsKey("mail_smtp_port")
                ? (String) settings.get("mail_smtp_port") : null;

        final String mailSmtpTls = settings.containsKey("mail_smtp_tlsenabled")
                ? (String) settings.get("mail_smtp_tlsenabled") : null;

        //send file via mail
        MailFacade facade = new MailFacade(mailSmtpHost, mailSmtpPort, mailSmtpTls, mailSmtpUser, mailSmtpPw, mailSmtpSender);

        if (mailAttachment) {
            final List<BodyPart> attachments = this.getAttachmentList(ctx, metaInfos, dataStreamMetaInfos, fileTemplate, numberLocale, headerTemplate, lineSeparator, lineTemplate, items);
            if(attachments != null && attachments.size() > 0){
                facade.send(mailReceiver,
                        mailSubject,
                        null,
                        attachments);
            } else {
                logger.info("No data found");
            }

        } else {
            final String body = this.getBody(ctx, metaInfos, dataStreamMetaInfos, fileTemplate, numberLocale, headerTemplate, lineSeparator, lineTemplate, items);
            if(StringUtils.isNotEmpty(body)){
                facade.send(mailReceiver,
                        mailSubject,
                        body,
                        null);
            } else {
                logger.info("No data found");
            }

        }

        return true;
    }

    /**
     * @param ctx
     * @param metaInfos
     * @param dataStreamMetaInfos
     * @param fileTemplate
     * @param numberLocale
     * @param headerTemplate
     * @param lineSeparator
     * @param lineTemplate
     * @param items
     * @return
     * @throws ProcessorException
     */
    private String getBody(
            RunnerContext ctx,
            final Map<String, String> metaInfos,
            final Map<Integer, Map<String, String>> dataStreamMetaInfos,
            final String fileTemplate,
            final String numberLocale,
            final String headerTemplate,
            final String lineSeparator,
            final String lineTemplate,
            Map<Integer, List<DataItem>> items) throws ProcessorException {

        final StringWriter out = new StringWriter();
        String fileName = null;
        Boolean addHeader = true;

        for (final Map.Entry<Integer, List<DataItem>> dataStream : items.entrySet()) {

            Map<String, String> mi = new HashMap<String, String>() {{
                putAll(metaInfos);
                putAll(dataStreamMetaInfos.get(dataStream.getKey()));
            }};

            String newFilename = TemplateParser.parse(
                    fileTemplate,
                    numberLocale,
                    mi,
                    ctx,
                    null);

            //add header based on filename
            if (fileName != null && fileName.equalsIgnoreCase(newFilename)) {
                addHeader = false;
            } else {
                addHeader = true;
                fileName = newFilename;
            }

            try {
                if (headerTemplate != null && addHeader) {
                    out.write(
                            TemplateParser.parse(
                                    headerTemplate,
                                    numberLocale,
                                    mi,
                                    ctx,
                                    null
                            ) + lineSeparator
                    );
                }

                for (DataItem item : dataStream.getValue()) {
                    out.write(
                            TemplateParser.parse(
                                    lineTemplate,
                                    numberLocale,
                                    mi,
                                    ctx,
                                    item) + lineSeparator);
                }

                //close writers
                out.close();

            } catch (IOException e) {
                throw new ProcessorException("Unable to store data to CSV: " + e.getMessage());
            } finally {}
        }

        return out.toString();
    }

    /**
     * @param ctx
     * @param metaInfos
     * @param dataStreamMetaInfos
     * @param fileTemplate
     * @param numberLocale
     * @param headerTemplate
     * @param lineSeparator
     * @param lineTemplate
     * @param items
     * @return
     * @throws ProcessorException
     */
    private List<BodyPart> getAttachmentList(
            RunnerContext ctx,
            final Map<String, String> metaInfos,
            final Map<Integer, Map<String, String>> dataStreamMetaInfos,
            final String fileTemplate,
            final String numberLocale,
            final String headerTemplate,
            final String lineSeparator,
            final String lineTemplate,
            Map<Integer, List<DataItem>> items) throws ProcessorException {

        final List<BodyPart> attachmentList = new ArrayList<>();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String filename = null;
        boolean fileExisting = false;
        try {
            for (final Map.Entry<Integer, List<DataItem>> dataStream : items.entrySet()) {

                Map<String, String> mi = new HashMap<String, String>() {{
                    putAll(metaInfos);
                    putAll(dataStreamMetaInfos.get(dataStream.getKey()));
                }};

                //creates new filename
                String newFilename = TemplateParser.parse(
                        fileTemplate,
                        numberLocale,
                        mi,
                        ctx,
                        null
                );

                if (filename == null) {
                    filename = newFilename;
                } else {
                    if (!filename.equalsIgnoreCase(newFilename)) {
                        //write data to attachment and add to list
                        attachmentList.add(createAttachment(filename, baos));
                        //create new instance
                        baos = new ByteArrayOutputStream();
                        filename = newFilename;
                    } else {
                        fileExisting = true;
                    }
                }

                PrintWriter out = new PrintWriter(baos);

                //add header line
                if (headerTemplate != null && !fileExisting) {
                    out.print(
                            TemplateParser.parse(
                                    headerTemplate,
                                    numberLocale,
                                    mi,
                                    ctx,
                                    null
                            ) + lineSeparator
                    );
                }

                //add rows
                for (DataItem item : dataStream.getValue()) {
                    out.print(
                            TemplateParser.parse(
                                    lineTemplate,
                                    numberLocale,
                                    mi,
                                    ctx,
                                    item) + lineSeparator);
                }

                out.close();
            }
            //write data to attachment and add to list
            attachmentList.add(createAttachment(filename, baos));

        } catch (IOException e) {
            throw new ProcessorException("Unable to store data to CSV: " + e.getMessage());
        } catch (MessagingException e) {
            throw new ProcessorException("Unable to send data with CSV: " + e.getMessage());
        } catch (Exception e) {
            throw new ProcessorException("Unknown error while creating CSV: " + e.getMessage());
        } finally {
            try {
                baos.close();
            } catch (Exception e) {
            }
        }

        return attachmentList;
    }

    /**
     * @param sFilename
     * @param bBaos
     * @return
     * @throws MessagingException
     * @throws IOException
     */
    private BodyPart createAttachment(final String sFilename, ByteArrayOutputStream bBaos) throws MessagingException, IOException {
        bBaos.close();
        BodyPart attachment = new MimeBodyPart();
        attachment.setDataHandler(new DataHandler(new ByteArrayDataSource(bBaos.toByteArray(), "text/csv")));
        attachment.setFileName(sFilename);
        return attachment;
    }
}
