FROM alpine:edge

COPY dist /app
WORKDIR /app

EXPOSE 6000

VOLUME [ "app/config" ]

CMD [ "./elastic-queue-logger" ]