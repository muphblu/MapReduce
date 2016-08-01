FROM python
RUN mkdir -p /src/ga1
RUN mkdir -p /src/ga1/files
RUN mkdir -p /src/ga1/map_reduce
COPY map_reduce/job_tracker.py /src/ga1/map_reduce/job_tracker.py
COPY mapper_content.py /src/ga1/
COPY mapreduce.py /src/ga1/
COPY master.py /src/ga1/
COPY naming_server.py /src/ga1/
COPY node1.py /src/ga1/
COPY node2.py /src/ga1/
COPY node3.py /src/ga1/
COPY node4.py /src/ga1/
COPY reducer_content.py /src/ga1/
COPY slave.py /src/ga1/
COPY storage_server.py /src/ga1/
COPY utils.py /src/ga1/
