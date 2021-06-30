FROM python:3.8.10-buster
COPY . ./quizzr-src
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
    cd gentle && ./install.sh && cd ..
RUN cd quizzr-src && \
    pip install -r requirements.txt; \
    [ -d privatedata ] && echo "WARNING: privatedata directory copied"; \
    [ ! -d recordings ] && mkdir recordings; [ ! -d privatedata ] && mkdir privatedata