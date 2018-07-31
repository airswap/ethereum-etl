FROM python:3.6

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD . .
CMD ["python3", "realtime.py"]