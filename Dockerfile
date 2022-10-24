FROM ubuntu:22.04

# Create user with home directory.
RUN useradd -mU sut
RUN useradd -mG sut contest

# Fix missing apt-utils.
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils

# Update packages.
RUN apt-get update && apt-get dist-upgrade -y

# Install allowed packages.
RUN apt-get install -y autoconf
RUN apt-get install -y automake
RUN apt-get install -y cmake
RUN apt-get install -y gcc
RUN apt-get install -y libjemalloc-dev
RUN apt-get install -y libboost-dev
RUN apt-get install -y clang
RUN apt-get install -y libtbb-dev
RUN apt-get install -y git
RUN apt-get install -y gdb

# Change user.
USER contest

WORKDIR /home/contest/dbtest