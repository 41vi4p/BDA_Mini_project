FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

# Debian Stretch (9) is EOL — replace sources.list with archive-only entries
RUN printf 'deb http://archive.debian.org/debian stretch main contrib non-free\n\
deb http://archive.debian.org/debian-security stretch/updates main\n' \
    > /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends python3 && \
    rm -rf /var/lib/apt/lists/*
