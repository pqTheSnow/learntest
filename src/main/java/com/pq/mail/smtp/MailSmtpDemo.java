package com.pq.mail.smtp;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author qiong.peng
 * @Date 2019/12/29
 */
public class MailSmtpDemo {
    public static void main(String[] args) {
        sendMailBySmtp();
    }

    public static void sendMailBySmtp() {
        sendMail("xxx@163.com", "email test", " test");
    }
    private static String from = "";
    private static String user = "";
    private static String password = "";
    /*
     * 读取属性文件的内容，并为上面上个属性赋初始值
     */
    static {
        Properties prop = new Properties();
        InputStream is = MailSmtpDemo.class.getClassLoader().getResourceAsStream("email.properties");
        try {
//            prop.load(is);
//            from = prop.getProperty("from");
//            user=prop.getProperty("username");
//            password=prop.getProperty("password");
            from = "xxx@qq.com";
            user = "xxx@qq.com";
            password = "xxx";
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void sendMail(String to,String text,String title) {
        Properties props = new Properties();
        props.setProperty("mail.smtp.host", "smtp.qq.com");//设置邮件服务器主机名
        props.put("mail.smtp.host", "smtp.qq.com");
        props.put("mail.smtp.auth", "true");//发送服务器需要身份验证
        props.put("mail.smtp.port", "465");
        props.put("mail.smtp.ssl.enable", true);
        Session session = Session.getDefaultInstance(props);//设置环境信息
        session.setDebug(true);
        MimeMessage message = new MimeMessage(session);
        Multipart multipart = null;
        BodyPart contentPart = null;
        Transport transport = null;
        try {
            message.setFrom(from);//设置发件人
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
            message.setSubject(title);
            multipart = new MimeMultipart();//设置附件
            contentPart = new MimeBodyPart();
            contentPart.setContent(text, "text/html;charset=utf-8");
            multipart.addBodyPart(contentPart);
            message.setContent(multipart);
            message.saveChanges();
            transport = session.getTransport("smtp");
            transport.connect("smtp.qq.com", 465, user, password);
            transport.sendMessage(message, message.getAllRecipients());
        } catch (MessagingException e) {

            e.printStackTrace();
        }finally {
            try {
                transport.close();
            } catch (MessagingException e) {
                e.printStackTrace();
            }
        }
    }
}
