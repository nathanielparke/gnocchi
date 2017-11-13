# This Dockerfile needs to be built with the Gnocchi root directory as
# the build context root directory.  To do this, navigate to the Gnocchi root
# directory, and then run (replace "<image tag>" with the tag you'd like to use
# for the image):
# $ docker build -t <image tag> -f kelda/Dockerfile .
#
# If you get an error message that looks like:
#   Error parsing reference: "keldaio/spark as builder"
#   is not a valid repository/tag: invalid reference format
# It means that you're running an older version of Docker
# that doesn't support using "as builder", and you'll
# need to upgrade.
FROM keldaio/spark as builder

RUN apt-get update
RUN apt-get install -y maven scala

# Copy and build the version of Gnocchi in the current directory.
RUN mkdir gnocchi
COPY . gnocchi

# On OSX with the default docker configuration this step fails because it runs
# out of memory.  In Docker -> Preferences -> Advanced you can change the
# memory limit to 3GB and it should work.
RUN cd gnocchi && mvn package -DskipTests

FROM keldaio/spark
COPY --from=builder ./gnocchi gnocchi/

ENV PATH /gnocchi/bin:$PATH
