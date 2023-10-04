FROM ubuntu:latest

#
# Install required software
#
RUN DEBIAN_FRONTEND=noninteractive apt update 

RUN DEBIAN_FRONTEND=noninteractive apt install sudo vim nano zip unzip tree iputils-ping net-tools wget openssh-server openjdk-8-jdk-headless ant maven yasm nasm cmake cmake-qt-gui g++ libgstreamer1.0-dev gstreamer1.0-plugins-base gstreamer1.0-plugins-good git-all bsdmainutils liblz4-tool lzop libisal-dev -y

#
# Configure SSH server to allow users to set environment variables
#
RUN sed -i 's/#PermitUserEnvironment no/PermitUserEnvironment yes/' /etc/ssh/sshd_config

#
# Configure SSH server to enable X11 forward
#
#X11Forwarding yes
#X11DisplayOffset 10
#X11UseLocalhost no

RUN sed -i 's/#X11DisplayOffset 10/X11DisplayOffset 10/' /etc/ssh/sshd_config
RUN sed -i 's/#X11UseLocalhost yes/X11UseLocalhost no/' /etc/ssh/sshd_config

#
# Set SSH as a service and start SSH server on port 22
#
RUN service ssh start
CMD [ "/usr/sbin/sshd", "-D" ]
EXPOSE 22

#
# Add a default user (non root) with sudo privileges
#
RUN useradd -rm -d /home/usermr -s /bin/bash -G sudo -u 1000 usermr
RUN echo 'usermr:m2preduce' | chpasswd

#
# Configure the previous user (usermr) to execute sudo commands without the need to enter his password
#
WORKDIR /etc/sudoers.d
COPY usermr .

#
# Copy base setup files for the home directory of the user (usermr)
#
WORKDIR /home/usermr
COPY .bash_aliases .
COPY bin.tar.gz .
COPY install-hadoop.tar.gz .
COPY setupSSH.sh .
COPY userKey.pub .

#
# Adjust file permissions
#
RUN sudo chown usermr:usermr .bash_aliases bin.tar.gz install-hadoop.tar.gz setupSSH.sh userKey.pub
RUN chmod +x setupSSH.sh
RUN chmod -x .bash_aliases bin.tar.gz install-hadoop.tar.gz userKey.pub

#
# Untar files
#
RUN tar -xzf bin.tar.gz
RUN rm -f bin.tar.gz
RUN tar -xzf install-hadoop.tar.gz
RUN rm -f install-hadoop.tar.gz

#
# Configure SSH authentication with public/private keys for user (usermr)
#
RUN mkdir -p ./.ssh
RUN mv userKey.pub ./.ssh
RUN ./setupSSH.sh
RUN sudo chown usermr:usermr ./.ssh/ ./.ssh/*

#
# Add the Hadoop scripts directory do the user (usermr) PATH
#
RUN echo "" >> .profile
RUN echo 'PATH=${HOME}/install-hadoop:${PATH}' >> .profile

#
# Expose Hadoop ports
#
# NameNode
EXPOSE 9864
EXPOSE 9868
EXPOSE 9870

# Resource Manager
EXPOSE 8042
EXPOSE 8088

# Job History
EXPOSE 19888