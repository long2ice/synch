FROM pypy:3
RUN mkdir -p /synch
WORKDIR /synch
COPY . /synch
RUN pypy3 -m pip install /synch