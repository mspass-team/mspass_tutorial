FROM wangyinz/mspass

RUN pip3 install jupyter

ADD scripts/start_mspass_tutorial.sh /usr/sbin/start_mspass_tutorial.sh
RUN chmod +x /usr/sbin/start_mspass_tutorial.sh

ADD notebooks notebooks

ENV PYSPARK_DRIVER_PYTHON jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS 'notebook --notebook-dir=/notebooks/ --port=8888 --no-browser --ip=0.0.0.0 --allow-root'

ENTRYPOINT ["/usr/sbin/start_mspass_tutorial.sh"]
