package com.surq.hdfs.file.merge
import org.apache.commons.mail.{ EmailException, DefaultAuthenticator, HtmlEmail }
import java.util.Properties

/**
 *  Created by 宿荣全 on 2018/12/11.
 * 初始化配置
 * @param USER_NAMRE 邮箱地址
 * @param PASSWD 邮箱密码
 * @param SMTP_ADDRESS  服务器地址
 * @param SMTP_PORT_SSL 端口号
 * @param FROM 发件人
 * @param SUBJECT 主题
 */
object MailUtil {
  /**
   *
   * @param toAddress 收件人地址
   * @param subject 主题
   * @param content 内容
   * @return
   */
  def sendHtmlMail(toAddress: Array[String], subject: String, content: String): Boolean = {
    val emailProperties = LoadFilePath.loadProperties("email.properties")
    val email = new HtmlEmail()

    email.setHostName(emailProperties.getProperty("SMTP_ADDRESS"))
    email.setSmtpPort(emailProperties.getProperty("SMTP_PORT_SSL").toInt)
    email.setCharset(emailProperties.getProperty("charset"))
    email.setAuthenticator(new DefaultAuthenticator(emailProperties.getProperty("USER_NAMRE"), emailProperties.getProperty("PASSWD")))
    email.setSSLOnConnect(true)
    try {
      email.setFrom(emailProperties.getProperty("FROM_EMAIL"), "基础平台")
      email.setSubject(subject)
      email.setHtmlMsg(content)
      toAddress.foreach(add => email.addTo(add))
      email.send()
      true
    } catch {
      case e: EmailException => false
    }
  }
}