FROM python:3-alpine
RUN pip3 install amqpy pymongo redis jinja2 pyyaml
COPY . /code
WORKDIR /code
CMD /bin/bash
