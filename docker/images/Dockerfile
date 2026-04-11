FROM rabbitmq:3-management

RUN mkdir -p /etc/rabbitmq/certs /var/log/rabbitmq

COPY ./certs/localhost.pem /etc/rabbitmq/certs
COPY ./certs/private_key.pem /etc/rabbitmq/certs

COPY rabbitmq.conf /etc/rabbitmq/rabbitmq.conf

ENV RABBITMQ_LOG_BASE=/var/log/rabbitmq

VOLUME ["/var/log/rabbitmq"]

EXPOSE 5671 5672 15672

CMD ["rabbitmq-server"]