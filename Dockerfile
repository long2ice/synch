FROM pypy:3
RUN mkdir -p /src
RUN mkdir ~/.pip
RUN echo "[global]\nindex-url = https://mirrors.aliyun.com/pypi/simple/\nformat = columns" > ~/.pip/pip.conf
WORKDIR /src
COPY requirements.txt /src
RUN pypy3 -m pip install -r requirements.txt
COPY . /src