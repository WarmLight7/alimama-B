FROM ubuntu:latest
ENV BUILD_CACHE=1
RUN printf "deb https://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse 	\n\
deb-src https://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse 		\n\
												\n\
deb https://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse 	\n\
deb-src https://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse 	\n\
												\n\
deb https://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse 	\n\
deb-src https://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse 	\n\
												\n\
deb https://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse 	\n\
deb-src https://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse 	\n\
" > /etc/apt/sources.list


#RUN cat /etc/apt/sources.list
RUN sed -i 's/https:\/\/mirrors.aliyun.com/http:\/\/mirrors.cloud.aliyuncs.com/g' /etc/apt/sources.list
RUN apt-get update -y
RUN apt-get install -y --no-install-recommends  build-essential 
RUN apt-get install -y --no-install-recommends cmake  gdb libboost-dev  
RUN apt-get install -y --no-install-recommends  autoconf automake
#RUN apt-get install -y  kernel-package linux-headers-`uname -r`
RUN apt-get install -y --no-install-recommends  net-tools  curl  netcat   inetutils-ping  vim
RUN apt-get install -y --no-install-recommends  libsnappy-dev  gnupg
RUN apt-get install -y --no-install-recommends  openjdk-8-jdk  
#
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
RUN curl -O https://dist.apache.org/repos/dist/release/hadoop/common/KEYS
RUN gpg --import KEYS
ENV HADOOP_VERSION=3.2.1
#ENV HADOOP_URL=https://www.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
ENV HADOOP_URL=https://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz
RUN curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz     && curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc     && gpg --verify /tmp/hadoop.tar.gz.asc     && tar -xvf /tmp/hadoop.tar.gz -C /opt/     && rm /tmp/hadoop.tar.gz*
RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop
RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs
RUN mkdir /hadoop-data
ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV PATH=/opt/hadoop-3.2.1/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin


RUN apt-get install -y libgrpc-dev libprotobuf-dev pkg-config protobuf-compiler protobuf-compiler-grpc  strace 
RUN apt-get install -y git
RUN git clone -c http.sslVerify=false https://github.com/grpc/grpc -b v1.30.x
RUN apt-get install -y libabsl-dev
RUN apt-get install -y libssl-dev
RUN apt-get install -y libc-ares-dev
RUN apt-get install -y libgrpc++ libgrpc++-dev
RUN apt-get install -y etcd

RUN apt-get install libboost-all-dev libssl-dev -y
RUN git clone https://github.com/microsoft/cpprestsdk.git
RUN bash -c 'cd cpprestsdk && mkdir build   && cd build  && cmake .. -DCPPREST_EXCLUDE_WEBSOCKETS=ON &&  make -j$(nproc) && make install'
RUN git clone https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git
RUN bash -c 'cd etcd-cpp-apiv3 &&  mkdir build &&  cd build && cmake .. && make -j$(nproc) && make install '

#RUN apt-get install python3 pip -y
#RUN mkdir ~/.pip/
#RUN echo "[global] \n\
#index-url=https://mirrors.cloud.aliyuncs.com/pypi/simple/ \n\
#\n\
#[install] \n\
#trusted-host=mirrors.cloud.aliyuncs.com" >> ~/.pip/pip.conf
#RUN python3 -m pip install sanic
#RUN python3 -m pip install requests
#RUN python3 -m pip install schema
#RUN python3 -m pip install asyncio

#RUN apt-get install -y bazel
#RUN for i in $(grep -rl '\-Wl' /usr/lib/x86_64-linux-gnu/pkgconfig/); do sed -i 's/-Wl//g' $i; done;

#RUN useradd -ms /bin/bash  app
#USER app
#WORKDIR /home/app
#RUN cd /home/app
#RUN git clone -c http.sslVerify=false -b v1.53.x https://github.com/grpc/grpc
#USER root

#RUN apt-get install -y golang git
#
#
#ENV GOPROXY=https://mirrors.aliyun.com/goproxy/
#ENV PATH=$PATH:/home/app/go/bin
#
#RUN go install github.com/bazelbuild/bazelisk@latest
#
