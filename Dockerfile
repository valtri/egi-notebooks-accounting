FROM python:3

COPY . /egi-notebooks-accounting

RUN pip install -e /egi-notebooks-accounting/
