FROM python:3

ADD reader.py /
RUN pip install azure
CMD ["python3", "./reader.py"]