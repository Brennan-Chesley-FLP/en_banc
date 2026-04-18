FROM cl-django:dev

USER root
RUN uv pip install --python /opt/venv/bin/python elasticsearch-dsl
USER www-data

ENTRYPOINT ["/bin/sh", "/opt/courtlistener/docker/django/docker-entrypoint.sh"]
