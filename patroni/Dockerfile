# PostgreSQL 16 managed by Patroni: the three members elect a leader
# through the 3-member etcd cluster and the others stream-replicate from
# it. If the leader dies, a replica is promoted automatically.
FROM postgres:16

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv curl \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/patroni \
 && /opt/patroni/bin/pip install --no-cache-dir \
        "patroni[etcd3]==4.0.4" psycopg2-binary

COPY patroni/patroni.yml /etc/patroni.yml
COPY patroni/post_bootstrap.sh /usr/local/bin/post_bootstrap.sh

RUN chmod 755 /usr/local/bin/post_bootstrap.sh \
 && mkdir -p /var/lib/postgresql/patroni \
 && chown -R postgres:postgres /var/lib/postgresql/patroni \
 && chmod 700 /var/lib/postgresql/patroni

USER postgres
EXPOSE 5432 8008

ENTRYPOINT []
CMD ["/opt/patroni/bin/patroni", "/etc/patroni.yml"]
