FROM docker.io/bitnami/spark:3

USER root 

#installing ssh server
RUN apt-get update && apt-get install -y openssh-server sudo

#bitnami uses uid 1001 as the non-root user, we are giving this uid a name and a password to be able to establish ssh and start history server
#adding 1001 to root as secondary group and giving it a name
RUN useradd -u 1001 -g 0 -m spark_user
#adding password to spark_user for ssh
RUN echo spark_user:Spark123@ | chpasswd
#adding spark_user to sudo group to start ssh from docker-compose
RUN usermod -aG sudo spark_user


USER spark_user

#creating development directory for spark_user
RUN mkdir -p /opt/bitnami/spark/dev
RUN mkdir /opt/bitnami/spark/dev/scripts /opt/bitnami/spark/dev/jars
#downloading requiered pip pkgs
COPY --chown=spark_user:root spark_requirement.txt /opt/bitnami/spark/dev/


USER root
RUN python3 -m pip install -r /opt/bitnami/spark/dev/spark_requirement.txt


USER spark_user
