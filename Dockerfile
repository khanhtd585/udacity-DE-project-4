FROM apache/airflow:2.7.1
COPY requirements.txt /requirements.txt
RUN pip3 install --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt