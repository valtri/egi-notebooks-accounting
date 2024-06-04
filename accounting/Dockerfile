FROM python:3

COPY .git/ accounting/ README.md LICENSE /egi-notebooks-accounting/

# avoid PEP 517 error
RUN pip install --no-cache-dir setuptools==58.2.0

RUN pip install --no-cache-dir -e /egi-notebooks-accounting/
