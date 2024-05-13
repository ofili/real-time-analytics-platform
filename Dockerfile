FROM ubuntu:latest
LABEL authors="ofili"

ENTRYPOINT ["top", "-b"]