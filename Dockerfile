FROM python:3.8.10-buster AS production
COPY . ./quizzr-src
RUN apt-get update \
    && apt-get --no-install-recommends install -y \
        gcc g++ gfortran \
        libc++-dev \
        libstdc++-7-dev \
        git \
        # nvidia-cuda-dev \
        ffmpeg \
        unzip \
    && git clone https://github.com/lowerquality/gentle.git \
    && pip install incremental \
    && cd gentle && ./install.sh
WORKDIR /quizzr-src
#RUN groupadd -r qserver && useradd --no-log-init -rm -g qserver qserver
#USER qserver
RUN mkdir -p \
        ~/quizzr_server/config/secrets \
        ~/quizzr_server/storage/queue \
    && pip install -r requirements.txt
#ENV PATH=$PATH:/home/qserver/.local/bin \
#    FLASK_APP=server
ENV FLASK_APP=server
CMD ["flask", "run", "--host=0.0.0.0"]

FROM production AS development
ENV FLASK_ENV=development

#Work in progress
#FROM development AS testing
#RUN git clone https://github.com/UMD-Summer-2021-ASR/quizzr-server-test
#WORKDIR /quizzr-src/quizzr-server-test
#ENV PYTHONPATH=/quizzr-src
#ENTRYPOINT ["pytest"]