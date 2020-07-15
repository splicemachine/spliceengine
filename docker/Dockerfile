FROM maven:3.6.3-jdk-8

# nc is used in start-splice-cluster to check if connection is opened -> service is ready
RUN apt-get update && apt-get install -y netcat

COPY entrypoint.sh /usr/local/bin/se-entrypoint.sh
RUN chmod +x /usr/local/bin/se-entrypoint.sh
ENTRYPOINT [ "/usr/local/bin/se-entrypoint.sh" ]
