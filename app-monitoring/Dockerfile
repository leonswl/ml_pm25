FROM python:3.9.8

WORKDIR /app/src

RUN apt-get update && apt-get upgrade -y
RUN pip install --no-cache -U pip
RUN pip install --no-cache poetry==1.7.1

# Configuring poetry.
RUN poetry config virtualenvs.create false

# First copy & install requirements to speed up the build process in case only the code changes.
COPY ./app-monitoring/pyproject.toml /app/src/
COPY ./app-monitoring/poetry.lock /app/src/

RUN poetry install --no-interaction --no-root -vvv

# Copy the rest of the files.
ADD ./app-monitoring /app/src

CMD ["streamlit", "run", "monitoring_src/main.py", "--server.port", "8502"]