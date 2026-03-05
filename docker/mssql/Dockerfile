FROM mcr.microsoft.com/mssql/server:2019-latest
USER root
WORKDIR /init
COPY ./initdb /initdb
COPY ./init-mssql.sh /init-mssql.sh
RUN chmod +x /init-mssql.sh
EXPOSE 1433
CMD ["/bin/bash", "/init-mssql.sh"]
