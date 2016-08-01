FROM python
RUN mkdir -p /src/ga1
COPY naming_server.py /src/ga1/
COPY client.py /src/ga1/
COPY storage_server.py /src/ga1/
COPY utils.py /src/ga1/
# todo add creation of the 'files' directory