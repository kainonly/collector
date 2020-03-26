FROM alpine:edge

COPY dist /app
WORKDIR /app

VOLUME [ "app/config" ]

CMD [ "./elastic-queue-logger" ]