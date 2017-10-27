FROM keldaio/spark as builder

RUN apt-get update
RUN apt-get install -y maven scala git
RUN git clone https://github.com/kelda-inc/gnocchi.git
RUN cd gnocchi && git checkout origin/gnocchiRefactor ## TODO delete me

# On OSX with the default docker configuration this step fails because it runs
# out of memory.  In Docker -> Preferences -> Advanced you can change the
# memory limit to 3GB and it should work.
RUN cd gnocchi && mvn package

FROM keldaio/spark
COPY --from=builder ./gnocchi gnocchi/

ENV PATH /gnocchi/bin:$PATH
