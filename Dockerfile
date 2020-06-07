FROM python:3.8
RUN mkdir -p /src
WORKDIR /src
COPY poetry.lock pyproject.toml /src/
ENV POETRY_VIRTUALENVS_CREATE=false
RUN pip3 install poetry
COPY . /src
RUN poetry install