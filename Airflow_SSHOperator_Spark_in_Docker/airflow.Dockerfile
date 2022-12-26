FROM apache/airflow:2.3.4
   
USER airflow

COPY pip_requirement.txt .
RUN python3 -m pip install -r airflow_requirement.txt

#creating public and private keys
RUN ssh-keygen -t ecdsa -b 521 -f /home/airflow/.ssh/id_ecdsa -N ''



