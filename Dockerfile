FROM python:3
RUN mkdir -p /synch
WORKDIR /synch
COPY . /synch
RUN pip3 install /synch