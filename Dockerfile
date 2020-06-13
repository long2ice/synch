FROM pypy:3
RUN mkdir -p /mysql2ch
WORKDIR /mysql2ch
COPY . /mysql2ch
RUN pypy3 -m pip install /mysql2ch