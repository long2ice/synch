FROM python:3.8
RUN mkdir -p /mysql2ch
WORKDIR /mysql2ch
RUN pip3 install git+https://github.com/long2ice/mysql2ch.git@dev