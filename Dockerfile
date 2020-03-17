FROM pypy:3
RUN mkdir -p /src
WORKDIR /src
COPY requirements.txt /src
RUN pypy3 -m pip install -r requirements.txt
COPY . /src