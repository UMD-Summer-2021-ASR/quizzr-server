FROM python:3.8.10-buster
COPY . ./quizzr-src
RUN cd quizzr-src && \
    pip install -r requirements.txt; \
    [ -d privatedata ] && echo "WARNING: privatedata directory copied"; \
    [ ! -d recordings ] && mkdir recordings; [ ! -d privatedata ] && mkdir privatedata
RUN apt-get update && \
    apt-get --no-install-recommends install -y \
        gcc g++ gfortran \
        libc++-dev \
        libstdc++-7-dev \
        git \
        # nvidia-cuda-dev \
        ffmpeg \
        unzip && \
    git clone https://github.com/lowerquality/gentle.git && \
    pip install incremental && \
    cd gentle && ./install.sh
WORKDIR /quizzr-src
ENV FLASK_APP=server FLASK_ENV=development
CMD ["flask", "run", "--host=0.0.0.0"]