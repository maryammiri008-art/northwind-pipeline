# از ایمیج رسمی Grafana به عنوان پایه استفاده میکنیم
FROM grafana/grafana:11.6.0

# کاربر root میشویم تا اجازه نصب پلاگین را داشته باشیم
USER root

# پوشه پلاگینها را ایجاد میکنیم
RUN mkdir -p /var/lib/grafana/plugins

# فایل ZIP پلاگین را از بیلد کانتکست به داخل ایمیج کپی میکنیم
COPY clickhouse-datasource-4.21.1.zip /tmp/

# فایل ZIP را در پوشه پلاگینها اکسترکت میکنیم
RUN unzip /tmp/clickhouse-datasource-4.21.1.zip -d /var/lib/grafana/plugins/

# (اختیاری) پاک کردن فایل ZIP برای کاهش حجم ایمیج
RUN rm -f /tmp/clickhouse-datasource-4.21.1.zip

# پورت 3000 را expose میکنیم
EXPOSE 3000
