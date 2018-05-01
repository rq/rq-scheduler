FROM python:3.5

COPY settings.conf.sample /scheduler/settings.conf
RUN pip install rq rq-scheduler

WORKDIR /scheduler

CMD rqscheduler `cat settings.conf`
