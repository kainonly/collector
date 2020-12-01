FROM alpine:edge

COPY dist /app
WORKDIR /app

EXPOSE 6000 8080

VOLUME [ "app/config" ]

CMD [ "./elastic-collector" ]