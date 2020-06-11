FROM pypy:3
ARG branch=master
RUN mkdir -p /mysql2ch
WORKDIR /mysql2ch
RUN pypy3 -m pip install git+https://github.com/long2ice/mysql2ch.git@${branch}