FROM python:3.8
WORKDIR /app
ADD . /app
EXPOSE 5000 5001
ENV sched "RR"
RUN pip3 install numpy
RUN echo ${sched}
CMD ["sh","-c","python3 master.py config.json ${sched}"]
