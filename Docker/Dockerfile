FROM python:3.6.6-alpine

WORKDIR /usr/src/app

COPY . .

# install dev tools
RUN apk update && apk add bash
RUN apk add gcc
RUN apk add linux-headers
RUN apk add musl-dev

# install & update pip
RUN python3 -m pip install --user --upgrade pip --force-reinstall

# create linux virtual venv and copy all source package from the windows env
RUN python3 -m pip install --user virtualenv
RUN python3 -m virtualenv venv
RUN cp -R env/Lib/site-packages/* venv/lib/python3.6/site-packages/

# reinstall psutil as linux distribution
RUN rm -rf venv/lib/python3.6/site-packages/psutil*
RUN source venv/bin/activate && pip install psutil --force-reinstall

# environment variables
ENV DEFAULT_EXCHANGES='Kraken/Huobi'
ENV LOG_LEVEL='info'
ENV SERVER_PORT=5000

# exchanges credentials in JSON format, e.g.: EXCHANGES_CREDENTIALS="{""Kraken"": {""apiKey"": ""432rfsdfsf"", ""secret"": ""fsdfasdfsda""}, ""Bitstamp"": {""username"": ""fsafsdaf"", ""key"": ""432rfsdfsf"", ""secret"": ""fsdfasdfsda""}}"
ENV EXCHANGES_CREDENTIALS=''

# expose web server default port
EXPOSE 5000

# configure volumes
VOLUME ["/usr/src/app/logs", "/usr/src/app/data"]

ENTRYPOINT ["bash", "-c", "./Docker/bootstrap.sh"]
