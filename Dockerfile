FROM python:3
RUN mkdir -p /synch
WORKDIR /synch
COPY . /synch
RUN pip3 install poetry
RUN poetry install -E mysql -E kafka -E postgres -E sentry