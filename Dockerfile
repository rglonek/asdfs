FROM ubuntu:24.04
RUN apt update && apt -y install wget fuse3
WORKDIR /root
RUN wget https://go.dev/dl/go1.23.4.linux-amd64.tar.gz
RUN tar -zxvf go1.23.4.linux-amd64.tar.gz
RUN echo 'PATH=/root/go/bin:$PATH' >> /root/.bashrc

