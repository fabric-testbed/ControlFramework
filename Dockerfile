FROM python:3.6-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN apk add gcc musl-dev python3-dev libffi-dev openssl-dev

RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install "connexion[swagger-ui]"

COPY . /usr/src/app

EXPOSE 8081

ENTRYPOINT ["python3"]

CMD ["-m", "actor.swagger_server"]
