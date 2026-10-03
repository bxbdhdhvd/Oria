# Multi-stage build: compile with the full toolchain, ship only the binary on the slim Swift runtime image.
#
#   docker build -t oria-example .
#   docker run --rm -p 3000:3000 --cpus=2 --memory=256m oria-example

# ---- Build ----
ARG SWIFT_IMAGE=swift:6.2-noble
FROM ${SWIFT_IMAGE} AS build
WORKDIR /src
# Resolve dependencies first so they are cached across source changes.
COPY Package.swift Package.resolved ./
RUN swift package resolve
COPY Sources ./Sources
COPY Tests ./Tests
RUN swift build -c release --product oria-example \
    && mkdir /out && cp "$(swift build -c release --show-bin-path)/oria-example" /out/

# ---- Runtime ----
# The slim image carries the Swift runtime libraries and nothing else (~100 MB compressed).
FROM swift:6.2-noble-slim
RUN useradd --system --uid 10001 --no-create-home oria && mkdir -p /data && chown oria /data
COPY --from=build /out/oria-example /usr/local/bin/oria-example
USER oria
ENV PORT=3000 FILES_DIR=/data
EXPOSE 3000
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/oria-example"]
