FROM wangyinz/mspass

RUN pip3 install jupyter

ADD scripts/start_mspass_tutorial.sh /usr/sbin/start_mspass_tutorial.sh
RUN chmod +x /usr/sbin/start_mspass_tutorial.sh

ENTRYPOINT ["/usr/sbin/start_mspass_tutorial.sh"]
