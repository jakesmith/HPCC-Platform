ARG IMAGE_BASE
FROM ${IMAGE_BASE}

ARG BASE_BRANCH_IMAGE
FROM ${BASE_BRANCH_IMAGE} as base_branch

COPY --from=base_branch /hpcc-dev/hpccinstall /hpcc-dev/hpccinstall
COPY --from=base_branch /hpcc-dev/HPCC-Platform/testing/regress /hpcc-dev/regress

# For debugging image only
USER root
RUN apt-get install -y sudo
RUN usermod -aG sudo hpcc
RUN echo "hpcc     ALL=(ALL) NOPASSWD:ALL"  >> /etc/sudoers
USER hpcc

COPY --chown=hpcc:hpcc entrypoint.sh /hpcc-dev/
RUN ls -lt /hpcc-dev/
RUN chmod +x /hpcc-dev/entrypoint.sh
RUN ls -lt /hpcc-dev/

WORKDIR /hpcc-dev/regress
COPY ./ecl-test.json .

ENTRYPOINT ["/hpcc-dev/entrypoint.sh"]
